_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {

class SnapTask {
public:
    SnapTask() = default;
    virtual ~SnapTask() = default;

    SnapTask(const SnapTask&) = delete;
    SnapTask& operator=(const SnapTask&) = delete;

    // 任务是否已经分发，避免重复分发(分发：投递给了worker）
    bool IsDispatched() const { return dispatched_; }
    void MarkAsDispatched() { dispatched_ = true; }

    virtual void Run() = 0;
    virtual void Cancel() = 0;

private:
    bool dispatched_ = false;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
