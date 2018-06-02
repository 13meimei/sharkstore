_Pragma("once");

#include <atomic>
#include <mutex>
#include <condition_variable>

#include "raft/src/impl/raft_types.h"
#include "raft/include/raft/statemachine.h"

#include "task.h"

namespace sharkstore {
namespace raft {
namespace impl {

using AckCallBack = std::function<void(const MessagePtr& ack_msg)>;

class ApplySnapTask : public SnapTask {
public:
    explicit ApplySnapTask(const SnapContext& context,
            const std::shared_ptr<StateMachine>& sm);
    ~ApplySnapTask();

    void SetAckCallBack(const AckCallBack ack_cb);
    Status RecvData(const MessagePtr& msg);

    void Run(SnapResult* result) override;

    void Cancel() override;
    bool IsCanceled() const { return canceled_; }

private:
    std::shared_ptr<StateMachine> sm_;
    AckCallBack  ack_callback_;

    int64_t last_apply_seq_ = 0;
    std::atomic<bool> canceled_ = {false};
    mutable std::mutex mu_;
    std::condition_variable cv_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
