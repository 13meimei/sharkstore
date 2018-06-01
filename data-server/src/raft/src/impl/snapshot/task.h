_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {

struct SnapResult {
    Status status;            // 执行结果, 出错情况
    size_t blocks_count = 0;  // 发送或者应用了多少数据块
    size_t bytes_count = 0;   // 发送或者应用了多少字节数
    int64_t seq = 0;          // 应用的序列号
};

struct SnapContext {
    uint64_t id = 0;
    uint64_t from = 0;
    uint64_t to = 0;
    uint64_t term = 0;
    uint64_t uuid = 0;
    pb::SnapshotMeta snap_meta;
};

using SnapReporter = std::function<void(const SnapContext&, const SnapResult&)>;

class SnapTask {
public:
    SnapTask(const SnapshotOptions& ops, const SnapContext& ctx)
        : opt_(opt), context_(ctx) {}

    virtual ~SnapTask() = default;

    SnapTask(const SnapTask&) = delete;
    SnapTask& operator=(const SnapTask&) = delete;

    // set before run
    void SetReporter(const SnapReporter& reporter) { repoter_ = reporter; }
    void Report(const SnapResult& result) { reporter_(header, result); }

    // 任务是否已经分发，避免重复分发(分发：投递给了worker）
    bool IsDispatched() const { return dispatched_; }
    void MarkAsDispatched() { dispatched_ = true; }

    virtual Run(SnapResult* result) = 0;
    virtual void Cancel() = 0;

protected:
    SnapshotOptions opt_;
    SnapContext context_;

private:
    SnapReporter reporter_;
    bool dispatched_ = false;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
