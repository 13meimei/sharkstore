_Pragma("once");

#include <stdint.h>
#include <string>

namespace fbase {
namespace raft {

enum class PeerType : char { kNormal, kLearner };

struct Peer {
    PeerType type = PeerType::kNormal;
    uint64_t node_id = 0;
    uint64_t peer_id = 0;
};

struct DownPeer {
    uint64_t node_id = 0;
    unsigned down_seconds = 0;
};

enum class ConfChangeType : char { kAdd, kRemove, kUpdate };

struct ConfChange {
    ConfChangeType type = ConfChangeType::kAdd;
    Peer peer;
    std::string context;
};

} /* namespace raft */
} /* namespace fbase */
