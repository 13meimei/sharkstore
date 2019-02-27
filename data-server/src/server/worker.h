_Pragma("once");

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <tbb/concurrent_queue.h>

#include "common/socket_message.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class RangeServer;

class Worker final {
public:
    Worker() = default;
    ~Worker() = default;

    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;

    Status Start(size_t fast_worker_size, size_t slow_worker_size, RangeServer* range_server);

    void Stop();

    void Push(common::ProtoMessage* msg);

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

        bool Push(common::ProtoMessage* msg);
        uint64_t PendingSize() const;
        uint64_t Clear();

    private:
        void runLoop();

    private:
        WorkThreadGroup* parent_group_ = nullptr;
        const size_t capacity_ = 0;
        tbb::concurrent_queue<common::ProtoMessage*> que_;
        std::thread thr_;
    };

    class WorkThreadGroup {
    public:
        WorkThreadGroup(RangeServer* rs, size_t num, size_t capacity_per_thread, const std::string& name);
        ~WorkThreadGroup();

        WorkThreadGroup(const WorkThreadGroup&) = delete;
        WorkThreadGroup& operator=(const WorkThreadGroup&) = delete;

        void Start();
        bool Push(common::ProtoMessage* msg);
        void DealTask(common::ProtoMessage* msg);
        uint64_t PendingSize() const;
        uint64_t Clear();

    private:
        RangeServer* const rs_;
        const size_t thread_num_ = 0;
        const size_t capacity_ = 0;
        const std::string name_;

        std::atomic<uint64_t> round_robin_counter_ = {0};
        std::vector<WorkThread*> threads_;
    };

private:
    static bool isSlowTask(common::ProtoMessage *task);

private:
    std::unique_ptr<WorkThreadGroup> fast_workers_;
    std::unique_ptr<WorkThreadGroup> slow_workers_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
