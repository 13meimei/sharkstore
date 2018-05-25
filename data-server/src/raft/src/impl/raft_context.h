_Pragma("once");

#include "snapshot_sender.h"
#include "transport/transport.h"
#include "work_thread.h"

namespace fbase {
namespace raft {
namespace impl {

struct RaftContext {
    WorkThread *consensus_thread{nullptr};
    WorkThread *apply_thread{nullptr};
    transport::Transport *msg_sender{nullptr};
    SnapshotSender *snap_sender{nullptr};
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
