_Pragma("once");

#include "raft_types.h"

namespace fbase {
namespace raft {
namespace impl {

struct Ready {
    // committed entries, can apply to statemachine
    std::vector<EntryPtr> committed_entries;

    // msgs to send
    std::vector<MessagePtr> msgs;
    std::shared_ptr<SnapshotRequest> snapshot;

    /* // change list about peers */
    /* std::vector<Peer> pendings_peers; */
    /* std::vector<DownPeers> down_peers; */
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
