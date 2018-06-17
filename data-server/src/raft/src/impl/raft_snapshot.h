_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <functional>
#include "raft/snapshot.h"
#include "raft_types.h"

namespace fbase {
namespace raft {
namespace impl {

struct SnapshotStatus {
    Status s;                 // 发送结果
    size_t blocks_count = 0;  // 总共发送了多少数据块
    size_t send_bytes = 0;    // 总共发送了多少字节数
};

// 快照发送结果报告
typedef std::function<void(const MessagePtr&, const SnapshotStatus&)>
    SnapStatusReporter;

struct SnapshotRequest {
public:
    SnapshotRequest();
    ~SnapshotRequest();

    void Cancel();
    bool Canceled() const;

    void Ack(int64_t seq, bool rejected);
    Status WaitAck(int64_t seq, int wait_sec);

public:
    MessagePtr header;
    std::shared_ptr<Snapshot> snapshot;
    SnapStatusReporter reporter;
    bool sending = false;

private:
    int64_t ack_seq_ = -1;
    bool rejected_ = false;
    bool cancel_ = false;
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

// 快照应用上下文
// 用于应用快照块时检查快照块的id是否一致, 序号seq是否连续
struct SnapshotApplyContext {
    uint64_t uuid = 0;
    pb::SnapshotMeta meta;
    int64_t prev_seq = 0;

    static std::atomic<uint64_t> total_applying;
    SnapshotApplyContext() { ++total_applying; };
    ~SnapshotApplyContext() { --total_applying; }
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
