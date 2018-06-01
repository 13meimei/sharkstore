_Pragma("once");

#include <condition_variable>
#include <mutex>

#include "../transport/transport.h"
#include "task.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

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
class SendTask : public Task {
public:
    SendTask(const SnapContext& SnapContext, const std::shared_ptr<Snapshot>& snap_data);
    ~SendTask();

    uint64_t UUID() const { return context_.uuid; }

    // 设置发送时需要的传输层接口，Run前先设置
    void SetTransport(transport::Transport* trans) { transport_ = trans; }

    // 收到副本的ack
    Status RecvAck(MessagePtr& msg);

    void Run(SnapResult* result) override;

    void Cancel() override;
    void IsCanceled() const { return canceled_; }

private:
    // 等待副本的ack
    Status waitAck(int64_t seq, int timeout_secs);

    // 准备下一个数据块, msg预先分配好内存，函数内赋值
    Status nextMsg(int64_t seq, MessagePtr& msg, bool* over);

private:
    std::shared_ptr<Snapshot> snap_data_;

    transport::Transport* transport_ = nullptr;

    int64_t ack_seq_ = 0;
    bool rejected_ = false;
    std::atomic<bool> canceled_ = {false};
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
