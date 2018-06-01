_Pragma("once");

#include "snapshot/manager.h"
#include "transport/transport.h"
#include "work_thread.h"

namespace sharkstore {
namespace raft {
namespace impl {

struct RaftContext {
    WorkThread *consensus_thread = nullptr;
    WorkThread *apply_thread = nullptr;
    transport::Transport *msg_sender = nullptr;
    snapshot::Manager *snapshot_manager = nullptr;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
