_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

class Task;
class WorkerPool;

class Worker final {
public:
    explicit Worker(WorkerPool* pool);
    ~Worker();

    Worker(const Worker&) = delete;
    Worker& operator=(const Worker&) = delete;

    void post(const std::shared_ptr<Task>& task);

private:
    void runTask();

private:
    WorkerPool* pool_ = nullptr;

    bool running_ = true;

    std::shared_ptr<Task> task_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::thread thr_;
};

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
