#include "raft_impl.h"

#include <sstream>
#include "bulletin_board.h"
#include "logger.h"
#include "raft_exception.h"
#include "raft_fsm.h"
#include "raft_types.h"
#include "storage/storage.h"

namespace fbase {
namespace raft {
namespace impl {

static const uint64_t kMaxSnapSizePerMsg = 1024 * 1024 * 1;

RaftImpl::RaftImpl(const RaftServerOptions& sops, const RaftOptions& ops,
                   const RaftContext& ctx)
    : sops_(sops),
      ops_(ops),
      ctx_(ctx),
      stopped_(false),
      fsm_(new RaftFsm(sops, ops)),
      bulletin_board_(new BulletinBoard) {
    // 初始化公告栏
    bulletin_board_->publish(fsm_->leader_, fsm_->term_);
    std::vector<Peer> peers;
    for (auto& p : fsm_->replicas_) {
        peers.push_back(fromPBPeer(p.second->peer()));
    }
    bulletin_board_->publish(std::move(peers));
}

RaftImpl::~RaftImpl() {
    delete fsm_;
    delete bulletin_board_;
}

void RaftImpl::GetLeaderTerm(uint64_t* leader, uint64_t* term) {
    bulletin_board_->leaderTerm(leader, term);
}

bool RaftImpl::IsLeader() { return sops_.node_id == bulletin_board_->leader(); }

Status RaftImpl::TryToLeader() {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }
    // 发送选举消息
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::LOCAL_MSG_HUP);
    msg->set_from(sops_.node_id);
    RecvMsg(msg);
    return Status::OK();
}

void RaftImpl::post(const std::function<void()>& f) {
    Work w;
    w.owner = ops_.id;
    w.stopped = &stopped_;
    w.f0 = f;
    ctx_.consensus_thread->post(w);
}

bool RaftImpl::tryPost(const std::function<void()>& f) {
    Work w;
    w.owner = ops_.id;
    w.stopped = &stopped_;
    w.f0 = f;
    return ctx_.consensus_thread->tryPost(w);
}

Status RaftImpl::Submit(std::string& cmd) {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }

    if (ctx_.consensus_thread->submit(
            ops_.id, &stopped_, std::bind(&RaftImpl::Step, shared_from_this(),
                                          std::placeholders::_1),
            cmd)) {
        return Status::OK();
    } else {
        return Status(Status::kBusy);
    }
}

Status RaftImpl::ChangeMemeber(const ConfChange& conf) {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }

    pb::ConfChange cc;
    toPBCC(conf, &cc);
    std::string str;
    if (!cc.SerializeToString(&str)) {
        return Status(Status::kCorruption, "protobuf serialize failed",
                      "ConfChange");
    }
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::LOCAL_MSG_PROP);
    auto entry = msg->add_entries();
    entry->set_type(pb::ENTRY_CONF_CHANGE);
    entry->mutable_data()->swap(str);

    if (tryPost(std::bind(&RaftImpl::Step, shared_from_this(), msg))) {
        return Status::OK();
    } else {
        return Status(Status::kBusy);
    }

    return Status::OK();
}

void RaftImpl::GetPeers(std::vector<Peer>* peers) {
    bulletin_board_->peers(peers);  // 更新peers
}

void RaftImpl::GetDownPeers(std::vector<DownPeer>* peers) {
    bulletin_board_->down_peers(peers);
}

void RaftImpl::GetPeedingPeers(std::vector<uint64_t>* peers) {
    bulletin_board_->pending_peers(peers);
}

void RaftImpl::GetStatus(RaftStatus* status) {
    bulletin_board_->status(status);
}

void RaftImpl::Truncate(uint64_t index) {
    post(std::bind(&RaftImpl::truncate, shared_from_this(), index));
}

void RaftImpl::RecvMsg(MessagePtr& msg) {
#ifdef FBASE_RAFT_TRACE_MSG
    if (msg->type() != pb::LOCAL_TICK) {
        LOG_DEBUG("recv msg type: %s from %llu, term: %llu at term: %llu",
                  pb::MessageType_Name(msg->type()).c_str(), msg->from(),
                  msg->term(), fsm_->term_);
    } else {
        LOG_DEBUG("recv msg type: LOCAL_TICK messages term: %llu at term: %llu",
                  msg->term(), fsm_->term_);
    }
#endif
    if (stopped_) return;

    // TODO: remove this
    if (msg->type() == pb::SNAPSHOT_RESPONSE) {  // 快照结果不丢弃
        post(std::bind(&RaftImpl::Step, shared_from_this(), msg));
    } else {
        if (!tryPost(std::bind(&RaftImpl::Step, shared_from_this(), msg))) {
            LOG_DEBUG(
                "raft[%llu] discard a msg. type: %s from %llu, term: %llu",
                ops_.id, pb::MessageType_Name(msg->type()).c_str(), msg->from(),
                msg->term());
        }
    }
}

void RaftImpl::Step(MessagePtr& msg) {
    if (!fsm_->validate(msg)) {
        LOG_DEBUG(
            "raft[%lu] ignore msg type: %s from %llu, term: %llu at term: %llu",
            ops_.id, pb::MessageType_Name(msg->type()).c_str(), msg->from(),
            msg->term(), fsm_->term_);
        return;
    }

    uint64_t prev_leader = fsm_->leader_;
    uint64_t prev_term = fsm_->term_;
    uint64_t prev_commit = fsm_->raft_log_->committed();
    uint64_t prev_vote = fsm_->vote_for_;

    // step
    fsm_->Step(msg);

    // update leader term
    maybeChange(prev_leader, prev_term);
    persist(prev_term, prev_commit, prev_vote);
    send();

    prev_leader = fsm_->leader_;
    prev_term = fsm_->term_;
    bool conf_changed = false;
    apply(&conf_changed);

    // 成员有变更，发布更新
    if (conf_changed) {
        // 如果是删除自己且自己是leader，会引起leader_更新
        maybeChange(prev_leader, prev_term);
        std::vector<Peer> peers;
        // 更新副本信息
        for (auto& p : fsm_->replicas_) {
            peers.push_back(fromPBPeer(p.second->peer()));
        }
        bulletin_board_->publish(std::move(peers));
    }

    // 更新RaftStatus 和 副本健康状态, 2s更新一次
    auto now = time(nullptr);
    if (now > last_update_status_ && now - last_update_status_ > 2) {
        last_update_status_ = now;

        RaftStatus s;
        fsm_->collect(&s);
        bulletin_board_->publish(std::move(s));

        if (IsLeader()) {
            bulletin_board_->publish(fsm_->down_peers_);
            bulletin_board_->publish(fsm_->pending_peers_);
        }
    }
}

void RaftImpl::ReportSnapshotStatus(const MessagePtr& header,
                                    const SnapshotStatus& ss) {
    if (ss.s.ok()) {
        LOG_INFO("raft[%llu] send snapshot[uuid: %lu] to %lu finished. total "
                 "blocks: %d, bytes: %d",
                 ops_.id, header->snapshot().uuid(), header->to(),
                 ss.blocks_count, ss.send_bytes);
    } else {
        LOG_ERROR("raft[%llu] send snapshot[uuid: %llu] to %lu failed(%s). "
                  "sent blocks: %d, bytes: %d",
                  ops_.id, header->snapshot().uuid(), header->to(),
                  ss.s.ToString().c_str(), ss.blocks_count, ss.send_bytes);
    }

    // 通知本地leader
    MessagePtr resp(new pb::Message);
    resp->set_type(pb::SNAPSHOT_RESPONSE);
    resp->set_to(header->from());
    resp->set_from(header->to());
    resp->set_term(header->term());
    resp->set_reject(!ss.s.ok());
    resp->mutable_snapshot()->set_uuid(header->snapshot().uuid());
    RecvMsg(resp);
}

void RaftImpl::maybeChange(uint64_t prev_leader, uint64_t prev_term) {
    if (fsm_->leader_ != prev_leader || fsm_->term_ != prev_term) {
        bulletin_board_->publish(fsm_->leader_, fsm_->term_);
        ops_.statemachine->OnLeaderChange(fsm_->leader_, fsm_->term_);
    }
}

void RaftImpl::persist(uint64_t prev_term, uint64_t prev_commit,
                       uint64_t prev_vote) {
    std::vector<EntryPtr> ents;
    fsm_->raft_log_->unstableEntries(&ents);
    if (!ents.empty()) {
        auto s = fsm_->storage_->StoreEntries(ents);
        if (!s.ok()) {
            throw RaftException(std::string("store entries failed: ") +
                                s.ToString());
        }
        fsm_->raft_log_->stableTo(ents.back()->index(), ents.back()->term());
#ifndef NDEBUG
        // 检查持久化正确性
        uint64_t index = 0;
        s = fsm_->storage_->LastIndex(&index);
        assert(s.ok());
        assert(index == ents.back()->index());
#endif
    }

    if (prev_term != fsm_->term_ ||
        prev_commit != fsm_->raft_log_->committed() ||
        prev_vote != fsm_->vote_for_) {
        pb::HardState hs;
        hs.set_term(fsm_->term_);
        hs.set_vote(fsm_->vote_for_);
        hs.set_commit(fsm_->raft_log_->committed());
        auto s = fsm_->storage_->StoreHardState(hs);
        if (!s.ok()) {
            throw RaftException(std::string("store hardstate failed: ") +
                                s.ToString());
        }
    }
}

void RaftImpl::smApply(const EntryPtr& e) {
    auto s = fsm_->smApply(e);
    if (!s.ok()) {
        throw RaftException(std::string("statemachine apply entry[") +
                            std::to_string(e->index()) + "] error: " +
                            s.ToString());
    }
}

void RaftImpl::apply(bool* conf_changed) {
    std::vector<EntryPtr> ents;
    fsm_->raft_log_->nextEntries(kNoLimit, &ents);
    for (const auto& e : ents) {
        if (e->type() == pb::ENTRY_CONF_CHANGE) {
            *conf_changed = true;
            auto s = fsm_->applyConfChange(e);
            if (!s.ok()) {
                throw RaftException(std::string("apply confchange[") +
                                    std::to_string(e->index()) + "] error: " +
                                    s.ToString());
            }
        }
        // 同步应用
        smApply(e);
        // 异步应用
        // Work w;
        // w.owner = ops_.id;
        // w.stopped = &stopped_;
        // w.f0 = std::bind(&RaftImpl::smApply, shared_from_this(), e);
        // ctx_.apply_thread->waitPost(w);
    }
    if (!ents.empty()) {
        fsm_->raft_log_->appliedTo(fsm_->raft_log_->committed());
    }
}

void RaftImpl::send() {
    // 发送消息
    if (!fsm_->pending_send_msgs_.empty()) {
        for (auto m : fsm_->pending_send_msgs_) {
            ctx_.msg_sender->SendMessage(m);
        }
        fsm_->pending_send_msgs_.clear();
    }

    // 发送快照
    if (fsm_->pending_send_snap_ && IsLeader() &&
        !fsm_->pending_send_snap_->sending) {
        fsm_->pending_send_snap_->sending = true;
        fsm_->pending_send_snap_->reporter =
            std::bind(&RaftImpl::ReportSnapshotStatus, shared_from_this(),
                      std::placeholders::_1, std::placeholders::_2);
        ctx_.snap_sender->Send(fsm_->pending_send_snap_);
    }
}

void RaftImpl::truncate(uint64_t index) {
    if (fsm_->applying_snap_) {
        return;
    }
    LOG_DEBUG("raft[%llu] truncate %llu", ops_.id, index);
    fsm_->storage_->Truncate(index);
}

void RaftImpl::Stop() { stopped_ = true; }

Status RaftImpl::BackupLog() { return fsm_->storage_->Backup(); }

Status RaftImpl::Destroy() { return fsm_->storage_->Destroy(); }

}  // namespace impl
}  // namespace raft
} /* namespace fbase */
