_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

struct SnapResult {
    Status status;            // 执行结果, 出错情况
    size_t blocks_count = 0;  // 发送或者应用了多少数据块
    size_t bytes_count = 0;   // 发送或者应用了多少字节数
};

struct SnapContext {
    uint64_t id = 0;
    uint64_t from = 0;
    uint64_t to = 0;
    uint64_t term = 0;
    uint64_t uuid = 0;
};

using Reporter = std::function<void(const SnapContext&, const SnapResult&)>;

class Task {
public:
    Task(const SnapContext& ctx, const SnapReporter& reporter)
        : context_(ctx), reporter_(reporter) {}

    virtual ~Task() = default;

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    virtual Run(SnapResult* result) = 0;
    virtual void Cancel() = 0;

    void Report(const SnapResult& result) { reporter_(header, result); }

    // 任务是否已经分发，避免重复分发(分发：投递给了worker）
    bool IsDispatched() const { return dispatched_; }
    void MarkAsDispatched() { dispatched_ = true; }

protected:
    SnapContext context_;

private:
    SnapReporter reporter_;
    bool dispatched_ = false;
};

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
