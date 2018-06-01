_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {

class WorkerPool;
class SendTask;
class ApplyTask;

class SnapshotManager final {
public:
    explicit SnapshotManager(const SnapshotOptions& opt);
    ~SnapshotManager();

    SnapshotManager(const SnapshotManager&) = delete;
    SnapshotManager& operator=(const SnapshotManager&) = delete;

    void Post(const std::shared_ptr<SendTask>& stask);
    void Post(const std::shared_ptr<ApplyTask>& atask);

private:
    const SnapshotOptions opt_;

    std::unique_ptr<WorkerPool> send_work_pool_;
    std::unique_ptr<WorkerPool> apply_work_pool_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
