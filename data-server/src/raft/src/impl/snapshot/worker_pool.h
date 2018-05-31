_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

class Worker;
class Task;

class WorkerPool final {
public:
    explicit WorkerPool(size_t size);
    ~WorkPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    bool Post(const std::shared_ptr<Task>& task);

    int Runnings();

private:
    friend class Worker;

    void addToFreeList(Worker* w);

private:
    std::vector<Worker*> workers_;

    std::list<Worker*> free_list_;
    mutable std::mutex mu_;
}

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
