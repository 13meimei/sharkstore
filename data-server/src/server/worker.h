_Pragma("once");

#include <thread>
#include <vector>
#include <atomic>
#include <tbb/concurrent_queue.h>

#include "common/rpc_request.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class RangeServer;

using RPCHandler = std::function<void(RPCRequest*)>;

class Worker final {
public:
    Worker() = default;
    ~Worker() = default;

    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;

    Status Start(int fast_worker_size, int slow_worker_size, RangeServer* range_server);
    void Stop();

    void Push(RPCRequest* req);

    void PrintQueueSize();
    size_t ClearQueue(bool fast, bool slow);
    uint64_t FastQueueSize() const { return fast_workers_->PendingSize(); }
    uint64_t SlowQueueSize() const { return slow_workers_->PendingSize(); }

private:
    class WorkThreadGroup;

    class WorkThread {
    public:
        WorkThread(WorkThreadGroup* group, const std::string& name, size_t max_capacity);
        ~WorkThread();

        WorkThread(const WorkThread&) = delete;
        WorkThread& operator=(const WorkThread&) = delete;

        bool Push(RPCRequest* req);
        uint64_t PendingSize() const;
        uint64_t Clear();

    private:
        void runLoop();

    private:
        WorkThreadGroup* parent_group_ = nullptr;
        const size_t capacity_ = 0;
        tbb::concurrent_queue<RPCRequest*> que_;
        std::thread thr_;
    };

    class WorkThreadGroup {
    public:
        WorkThreadGroup(RangeServer* rs, int num, size_t capacity_per_thread, const std::string& name);
        ~WorkThreadGroup();

        WorkThreadGroup(const WorkThreadGroup&) = delete;
        WorkThreadGroup& operator=(const WorkThreadGroup&) = delete;

        void Start();
        bool Push(RPCRequest* msg);
        void DealTask(RPCRequest* req_ptr);
        uint64_t PendingSize() const;
        uint64_t Clear();

    private:
        RangeServer* const rs_;
        const int thread_num_ = 0;
        const size_t capacity_ = 0;
        const std::string name_;

        std::atomic<uint64_t> round_robin_counter_ = {0};
        std::vector<WorkThread*> threads_;
    };

private:
    static bool isSlowTask(RPCRequest *task);

private:
    std::unique_ptr<WorkThreadGroup> fast_workers_;
    std::unique_ptr<WorkThreadGroup> slow_workers_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
