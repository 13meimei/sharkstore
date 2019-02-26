_Pragma("once");

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <semaphore.h>

#include "common/socket_message.h"
#include "lk_queue/lk_queue.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class Worker final {
public:
    Worker(size_t fast_worker_size, size_t slow_worker_size);
    ~Worker() = default;

    Worker(const Worker &) = delete;
    Worker &operator=(const Worker &) = delete;

    int Start();
    void Stop();

    // 0: fast queue; 1: slow queue; 2: thread queue
    void Push(const common::ProtoMessage& task);

    void PrintQueueSize();

    size_t ClearQueue(bool fast, bool slow);

    uint64_t FastQueueSize() const { return fast_queue_.all_msg_size; }
    uint64_t SlowQueueSize() const { return slow_queue_.all_msg_size; }

private:
    struct HashQueue {
        std::vector<lock_free_queue_t *> msg_queue;
        std::atomic<uint64_t> all_msg_size;

        HashQueue() : all_msg_size(0) {}
    };

    bool isSlow(common::ProtoMessage *msg);

    void DealTask(common::ProtoMessage *task);
    void Clean(HashQueue &hash_queue);

    void StartWorker(std::vector<std::thread> &worker, HashQueue & hash_queue, int num);

private:
    const size_t fast_worker_size_ = 0;
    const size_t slow_worker_size_ = 0;

    std::atomic<uint64_t> slot_seed_;

    HashQueue fast_queue_;
    HashQueue slow_queue_;

    std::vector<std::thread> fast_workers_;
    std::vector<std::thread> slow_workers_;
};

} /* namespace server */
} /* namespace dataserver  */
} /* namespace sharkstore */
