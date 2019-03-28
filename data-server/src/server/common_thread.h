_Pragma("once");

#include <thread>
#include <condition_variable>
#include <mutex>
#include <assert.h>
#include <atomic>
#include <memory>
#include <queue>
#include <functional>
#include <unordered_map>
#include "base/util.h"
#include "raft_logger.h"

namespace sharkstore {
namespace dataserver {

static const int kMaxBatchSize = 64;

struct Work {
    uint64_t owner = 0;
    std::atomic<bool>* stopped = nullptr;

    std::function<void()> f0;

    void Do();
};

class WorkThread {
public:
    WorkThread(size_t queue_capcity, const std::string& name = "worker");
    ~WorkThread();

    WorkThread(const WorkThread&) = delete;
    WorkThread& operator=(const WorkThread&) = delete;

    bool submit(uint64_t owner, std::atomic<bool>* stopped,
                const std::function<void()>& f0, std::string& cmd);

    bool tryPost(const Work& w);
    void post(const Work& w);
    void waitPost(const Work& w);
    void shutdown();
    int size() const;

private:
    bool pull(Work* w);
    void run();

private:
    const size_t capacity_ = 0;

    std::unique_ptr<std::thread> thr_;
    bool running_ = false;
    std::queue<Work> queue_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
};


} /* namespace dataserver */
} /* namespace sharkstore */
