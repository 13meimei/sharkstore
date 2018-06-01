_Pragma("once");

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

class Worker;
class SnapTask;

class WorkerPool final {
public:
    explicit WorkerPool(const std::string& name, size_t size);
    ~WorkPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    bool Post(const std::shared_ptr<SnapTask>& task);
    int Runnings() const;

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
