_Pragma("once");

#include <condition_variable>
#include <mutex>
#include <atomic>

#include "raft/include/raft/snapshot.h"

#include "../transport/transport.h"
#include "task.h"
#include "types.h"

namespace sharkstore {
namespace raft {
namespace impl {

/**
 * 一次快照的发送拆分成连续多条Message来发送
 * 每条Snapshot Message的uuid一样，seq递增
 * 每条Message发送完等待对端的Ack后再发下一条
 * 如果等待Ack超时则发送失败
 *
 * Message [ seq-1 snapshot header ] (snapshot meta)
 * Message [ seq-2 snapshot data block ]
 *   .
 *   .
 *   .
 * Message [ seq-n snapshot datat block ]
 */

class SendSnapTask : public SnapTask {
public:
    struct Options {
        size_t max_size_per_msg = 0;
        size_t wait_ack_timeout_secs = 0;
    };

    SendSnapTask(const SnapContext& context,
                 pb::SnapshotMeta& meta,
                 const std::shared_ptr<Snapshot>& data);
    ~SendSnapTask();

    SnapContext& GetContext() const { return context_; }

    // 设置发送时需要的传输层接口，Dispatch前先设置
    void SetTransport(transport::Transport* trans) { transport_ = trans; }
    // 设置发送结果汇报回调
    void SetReporter(const SnapReporter& reporter) { reporter_ = reporter; }
    // 设置发送选项
    void SetOptions(const Options& opt) { opt_ = opt };

    // 收到副本的ack
    Status RecvAck(MessagePtr& msg);

    void Run() override;
    void Cancel() override;
    bool IsCanceled() const { return canceled_; }

private:
    void run(SnapResult* result);

    // 等待副本的ack
    Status waitAck(int64_t seq, int timeout_secs);

    // 准备下一个数据块, msg预先分配好内存，函数内赋值
    Status nextMsg(int64_t seq, MessagePtr& msg, bool* over);

private:
    SnapContext context_;
    pb::SnapshotMeta meta_; // 使用一次后即失效（被Swap）
    std::shared_ptr<Snapshot> data_;

    transport::Transport* transport_ = nullptr;
    SnapReporter  reporter_;
    Options opt_;

    int64_t ack_seq_ = 0;
    bool rejected_ = false;
    std::atomic<bool> canceled_ = {false};
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
