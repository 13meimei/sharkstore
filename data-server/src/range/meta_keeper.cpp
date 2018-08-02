#include "meta_keeper.h"

namespace sharkstore {
namespace dataserver {
namespace range {

MetaKeeper::MetaKeeper(const metapb::Range& m) : meta_(m) {}

MetaKeeper::MetaKeeper(metapb::Range&& m) : meta_(std::move(m)) {}

metapb::Range MetaKeeper::Get() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    auto result = meta_;
    return result;
}

void MetaKeeper::Set(const metapb::Range& to) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
    meta_ = to;
}

void MetaKeeper::Set(metapb::Range&& to) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
    meta_ = std::move(to);
}

uint64_t MetaKeeper::GetConfVer() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return meta_.range_epoch()->conf_ver();
}

uint64_t MetaKeeper::GetVersion() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return meta_.range_epoch()->version();
}

Status MetaKeeper::verifyConfVer(uint64_t conf_ver) {
    uint64_t current_ver = meta_.range_epoch().conf_ver();
    if (conf_ver == current_ver) {
        return Status::OK();
    }

    std::ostringstream ss;
    ss << "current is: " << current_ver;
    ss << ", to verify is: " << conf_ver;

    if (ver < current_ver) {
        return Status(Status::kStaleEpoch, "conf ver", ss.str());
    } else {
        assert(conf_ver != current_ver);
        return Status(Status::kInvalidArgument, "conf ver", ss.str());
    }
}

Status MetaKeeper::AddPeer(const metapb::Peer& peer, uint64_t verify_conf_ver) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

    auto s = verifyConfVer(verify_conf_ver);
    if (!s.ok()) return s;

    // check peer already exist
    for (const auto& old : meta_.peers()) {
        if (old.node_id() == peer.node_id() || old.id() == peer.id()) {
            return Status(Status::kExisted,
                    std::string("old is ") + old.ShortDebugString(),
                    std::string("to add is ") + peer.ShortDebugString());
        }
    }

    // add
    meta_.add_peers()->CopyFrom(peer);
    meta_.mutable_range_epoch()->set_conf_ver(verify_conf_ver + 1);

    return Status::OK();
}

Status MetaKeeper::DelPeer(const metapb::Peer& peer, uint64_t verify_conf_ver) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

    auto s = verifyConfVer(verify_conf_ver);
    if (!s.ok()) return s;

    // find and delete
    for (auto it = meta_.peers().cbegin(); it != meta_.peers().cend(); ++it) {
        auto same_node = it->node_id() == peer().node_id();
        auto same_id = it->id() == peer.id();
        if (same_node && same_id) {
            meta_.mutable_peers()->erase(it);
            meta_.mutable_range_epoch()->set_conf_ver(verify_conf_ver + 1);
            return Status::OK();
        } else if (same_node || same_id) {
            std:ostringst
            return Status(Status::kInvalidArgument, "mismatch", )
        }
    }
    return Status(Status::kNotFound);
}

Status MetaKeeper::PromotePeer(const metapb::Peer& peer) {
    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

    return Status::OK();
}

void MetaKeeper::GetAllPeers(std::vector<metapb::Peer>* peers) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    for (const auto& p : meta_.peers()) {
        peers->emplace_back(p);
    }
}

bool MetaKeeper::FindPeerByNodeID(uint64_t node_id, metapb::Peer* peer) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    for (const auto& p: meta_.peers()) {
        if (p.node_id() == node_id) {
            if (peer != nullptr) peer->CopyFrom(p);
            return true;
        }
    }
    return false;
}

bool MetaKeeper::FindPeerByPeerID(uint64_t peer_id, metapb::Peer* peer) const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    for (const auto& p: meta_.peers()) {
        if (p.id() == peer_id) {
            if (peer != nullptr) peer->CopyFrom(p);
            return true;
        }
    }
    return false;
}

std::string MetaKeeper::ToString() const {
    std::string s;
    {
        sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
        s = meta_.ShortDebugString();
    }
    return s;
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
