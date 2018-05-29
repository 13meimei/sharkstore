#include "range.h"

#include "storage/meta_store.h"

namespace sharkstore {
namespace dataserver {
namespace range {

void Range::AddPeer(const metapb::Peer &peer) {
    raft::ConfChange cch;

    auto fp = FindMetaPeer(peer.node_id());
    if (fp != nullptr) {
        FLOG_WARN("Range %" PRIu64 " AddPeer NodeId: %" PRIu64 " existed", meta_.id(),
                  peer.node_id());
        return;
    }

    cch.type = raft::ConfChangeType::kAdd;

    cch.peer.type = raft::PeerType::kNormal;
    cch.peer.node_id = peer.node_id();
    cch.peer.peer_id = peer.id();

    raft_cmdpb::PeerTask pt;

    auto ap = pt.mutable_peer();
    ap->set_id(peer.id());
    ap->set_node_id(peer.node_id());
    ap->set_type(peer.type());

    auto ep = pt.mutable_verify_epoch();
    ep->set_conf_ver(meta_.range_epoch().conf_ver());

    pt.SerializeToString(&cch.context);
    raft_->ChangeMemeber(cch);

    FLOG_INFO("Range %" PRIu64 " AddPeer NodeId: %" PRIu64 " version:%" PRIu64
              " conf_ver:%" PRIu64,
              meta_.id(), peer.node_id(), meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());
}

void Range::DelPeer(const metapb::Peer &peer) {
    raft::ConfChange cch;

    auto fp = FindMetaPeer(peer.node_id());
    if (fp == nullptr || fp->id() != peer.id()) {
        FLOG_WARN("Range %" PRIu64 " DelPeer NodeId: %" PRIu64 " peer:%" PRIu64
                  " info mismatch!",
                  meta_.id(), peer.node_id(), peer.id());
        return;
    }

    cch.type = raft::ConfChangeType::kRemove;

    cch.peer.type = raft::PeerType::kNormal;
    cch.peer.node_id = peer.node_id();
    cch.peer.peer_id = peer.id();
    raft_cmdpb::PeerTask pt;

    auto ap = pt.mutable_peer();
    ap->set_id(peer.id());
    ap->set_node_id(peer.node_id());
    ap->set_type(peer.type());

    auto ep = pt.mutable_verify_epoch();
    ep->set_conf_ver(meta_.range_epoch().conf_ver());

    pt.SerializeToString(&cch.context);
    raft_->ChangeMemeber(cch);

    FLOG_INFO("Range %" PRIu64 " DelPeer NodeId: %" PRIu64 " version:%" PRIu64
              " conf_ver:%" PRIu64,
              meta_.id(), peer.node_id(), meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());
}

Status Range::ApplyMemberChange(const raft::ConfChange &cc, uint64_t index) {
    Status ret;
    switch (cc.type) {
        case raft::ConfChangeType::kAdd:
            ret = ApplyAddPeer(cc);
            break;
        case raft::ConfChangeType::kRemove:
            ret = ApplyDelPeer(cc);
            break;
        default:
            ret =
                Status(Status::kNotSupported, "Apply member change not support type", "");
    }
    if (!ret.ok()) return ret;

    apply_index_ = index;
    auto s = context_->meta_store->SaveApplyIndex(meta_.id(), apply_index_);
    if (!s.ok()) {
        FLOG_ERROR("range[%" PRIu64 "] save apply index error %s", meta_.id(),
                   s.ToString().c_str());
        return s;
    }
    return Status::OK();
}

Status Range::ApplyAddPeer(const raft::ConfChange &cc) {
    FLOG_INFO("Range %" PRIu64 " ApplyAddPeer NodeId: %" PRIu64 " Begin, version:%" PRIu64
              " conf_ver:%" PRIu64,
              meta_.id(), cc.peer.node_id, meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());

    raft_cmdpb::PeerTask pt;

    if (!context_->socket_session->GetMessage(cc.context.c_str(), cc.context.size(),
                                              &pt)) {
        FLOG_ERROR("Range %" PRIu64 " ApplyAddPeer NodeId: %" PRIu64 " deserialize fail",
                   meta_.id(), cc.peer.node_id);
        return Status(Status::kInvalidArgument, "context deserialize fail", "");
    }

    if (pt.verify_epoch().conf_ver() < meta_.range_epoch().conf_ver()) {
        FLOG_INFO("Range %" PRIu64 " ApplyAddPeer NodeId: %" PRIu64
                  " epoch stale, verify: %" PRIu64 ", cur: %" PRIu64,
                  meta_.id(), cc.peer.node_id, pt.verify_epoch().conf_ver(),
                  meta_.range_epoch().conf_ver());
        return Status::OK();
    } else if (pt.verify_epoch().conf_ver() > meta_.range_epoch().conf_ver()) {
        FLOG_ERROR("Range %" PRIu64 " ApplyAddPeer NodeId: %" PRIu64
                   " epoch stale, verify: %" PRIu64 ", cur: %" PRIu64,
                   meta_.id(), cc.peer.node_id, pt.verify_epoch().conf_ver(),
                   meta_.range_epoch().conf_ver());
        return Status(Status::kStaleEpoch, "stale epoch", "");
    }

    bool is_modify = false;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(meta_lock_);

        auto fp = FindMetaPeer(cc.peer.node_id);
        if (fp != nullptr) {
            FLOG_WARN("Range %" PRIu64 " ApplyAddPeer NodeId: %" PRIu64 " is existed",
                      meta_.id(), cc.peer.node_id);
            return Status::OK();
        }

        metapb::Range meta = meta_;
        AddPeer(pt, meta);
        if (SaveMeta(meta)) {
            AddPeer(pt, meta_);
            is_modify = true;
        }
    } while (false);

    // send Heartbeat message
    if (is_modify) {
        PushHeartBeatMessage();
    }

    FLOG_INFO("Range %" PRIu64 " ApplyAddPeer NodeId: %" PRIu64 " End, version:%" PRIu64
              " conf_ver:%" PRIu64,
              meta_.id(), cc.peer.node_id, meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());

    return Status::OK();
}

Status Range::ApplyDelPeer(const raft::ConfChange &cc) {
    FLOG_INFO("Range %" PRIu64 " ApplyDelPeer NodeId:%" PRIu64 " Begin, version:%" PRIu64
              " conf_ver:%" PRIu64,
              meta_.id(), cc.peer.node_id, meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());

    raft_cmdpb::PeerTask pt;

    if (!context_->socket_session->GetMessage(cc.context.c_str(), cc.context.size(),
                                              &pt)) {
        FLOG_ERROR("Range %" PRIu64 " ApplyDelPeer NodeId: %" PRIu64 " deserialize fail",
                   meta_.id(), cc.peer.node_id);
        return Status(Status::kInvalidArgument, "context deserialize fail", "");
    }

    if (pt.verify_epoch().conf_ver() < meta_.range_epoch().conf_ver()) {
        FLOG_INFO("Range %" PRIu64 " ApplyDelPeer NodeId: %" PRIu64
                  " epoch stale, verify: %" PRIu64 ", cur: %" PRIu64,
                  meta_.id(), cc.peer.node_id, pt.verify_epoch().conf_ver(),
                  meta_.range_epoch().conf_ver());
        return Status::OK();
    } else if (pt.verify_epoch().conf_ver() > meta_.range_epoch().conf_ver()) {
        FLOG_ERROR("Range %" PRIu64 " ApplyDelPeer NodeId: %" PRIu64
                   " epoch stale, verify: %" PRIu64 ", cur: %" PRIu64,
                   meta_.id(), cc.peer.node_id, pt.verify_epoch().conf_ver(),
                   meta_.range_epoch().conf_ver());
        return Status(Status::kStaleEpoch, "stale epoch", "");
    }

    bool is_modify = false;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(meta_lock_);

        metapb::Range meta = meta_;
        if (DelPeer(pt, meta)) {
            if (SaveMeta(meta)) {
                is_modify = DelPeer(pt, meta_);
            }
        }
    } while (false);

    if (is_modify) {
        // send Heartbeat message
        PushHeartBeatMessage();
    } else {
        FLOG_WARN("Range %" PRIu64 " ApplyDelPeer Not Found NodeId: %" PRIu64, meta_.id(),
                  cc.peer.node_id);
    }

    FLOG_INFO("Range %" PRIu64 " ApplyDelPeer NodeId: %" PRIu64 " End, version:%" PRIu64
              " conf_ver:%" PRIu64,
              meta_.id(), cc.peer.node_id, meta_.range_epoch().version(),
              meta_.range_epoch().conf_ver());

    return Status::OK();
}

void Range::AddPeer(raft_cmdpb::PeerTask &pt, metapb::Range &meta) {
    auto ap = meta.add_peers();
    ap->set_id(pt.peer().id());
    ap->set_node_id(pt.peer().node_id());
    ap->set_type(pt.peer().type());

    auto ver = pt.verify_epoch().conf_ver() + 1;
    meta.mutable_range_epoch()->set_conf_ver(ver);
}

bool Range::DelPeer(raft_cmdpb::PeerTask &pt, metapb::Range &meta) {
    auto peers = meta.mutable_peers();
    auto it = peers->begin();
    while (it != peers->end()) {
        if (it->node_id() == pt.peer().node_id() && it->id() == pt.peer().id()) {
            peers->erase(it);
            auto ver = pt.verify_epoch().conf_ver() + 1;
            meta.mutable_range_epoch()->set_conf_ver(ver);
            return true;
        }
        ++it;
    }

    return false;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
