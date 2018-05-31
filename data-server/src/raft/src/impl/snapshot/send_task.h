_Pragma("once");

#include <condition_variable>
#include <mutex>

#include "../transport/transport.h"
#include "task.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

class SendTask : public Task {
public:
    SendTask(const SnapContext& SnapContext, const SnapReporter& reporter,
             pb::SnapshotMeta& snap_meta, const std::shared_ptr<Snapshot>& snap_data);
    ~SendTask();

    uint64_t UUID() const { return context_.uuid; }

    void SetTransport(transport::Transport* trans) { transport_ = trans; }

    // 收到副本的ack
    void RecvAck(int64_t seq, bool rejected);

    void Run(SnapResult* result) override;
    void Cancel() override;
    void IsCanceled() const;

private:
    // 等待副本的ack
    Status waitAck(int64_t seq, int timeout_secs);
    std::tuple<MessagePtr, bool> nextMsg(int64_t seq, uint64_t max_size);

private:
    pb::SnapshotMeta snap_meta_;
    std::unique_ptr<Snapshot> snap_data_;

    transport::Transport* transport_ = nullptr;

    int64_t ack_seq_ = -1;
    bool rejected_ = false;
    bool canceled_ = false;
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
