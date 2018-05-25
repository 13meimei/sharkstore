_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "base/status.h"
#include "raft/snapshot.h"

#include "raft_snapshot.h"
#include "raft_types.h"
#include "transport/transport.h"

namespace fbase {
namespace raft {
namespace impl {

class SnapshotSender {
public:
    SnapshotSender(transport::Transport *transport, uint8_t concurrency,
                   size_t max_size_per_msg = 64 * 1024);
    ~SnapshotSender();

    SnapshotSender(const SnapshotSender &) = delete;
    SnapshotSender &operator=(const SnapshotSender &) = delete;

    Status Start();
    Status ShutDown();

    void Send(std::shared_ptr<SnapshotRequest> snap);
    size_t GetConcurrency() const;

private:
    void send_loop();

    void send_snapshot(transport::Connection &conn, SnapshotRequest &snap,
                       SnapshotStatus *status);

    void do_send(SnapshotRequest &snap, SnapshotStatus *status);

private:
    transport::Transport *transport_ = nullptr;
    const uint8_t concurrency_ = 0;
    const size_t max_size_per_msg_ = 0;

    std::atomic<bool> running_ = {true};
    std::vector<std::thread *> send_threads_;
    std::queue<std::shared_ptr<SnapshotRequest>> queue_;
    size_t sending_count_ = 0;
    mutable std::mutex mu_;
    std::condition_variable cond_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
