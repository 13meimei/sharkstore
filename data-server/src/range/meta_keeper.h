_Pragma("once");

#include "proto/gen/metapb.pb.h"

namespace sharkstore {
namespace dataserver {
namespace range {

// MetaKeeper:
// manager query and update operation to metapb::Range
class MetaKeeper {
public:
    explicit MetaKeeper(const metapb::Range& m);
    explicit MetaKeeper(metapb::Range&& m);
    ~MetaKeeper() = default;

    MetaKeeper(const MetaKeeper&) = delete;
    MetaKeeper& operator=(const MetaKeeper&) = delete;

    metapb::Range Get() const;
    void Set(const metapb::Range& to);
    void Set(metapb::Range&& to);

    uint64_t GetConfVer() const;
    uint64_t GetVersion() const;

    Status AddPeer(const metapb::Peer& peer, uint64_t verify_conf_ver);
    Status DelPeer(const metapb::Peer& peer, uint64_t verify_conf_ver);
    Status PromotePeer(const metapb::Peer& peer);

    void GetAllPeers(std::vector<metapb::Peer>* peers) const;
    bool FindPeerByNodeID(uint64_t node_id, metapb::Peer* peer = nullptr) const;
    bool FindPeerByPeerID(uint64_t peer_id, metapb::Peer* peer = nullptr) const;

    std::string ToString() const;

private:
    // unlocked
    Status verifyConfVer(uint64_t conf_ver);

private:
    mutable shared_mutex rw_lock_;
    metapb::Range meta_;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
