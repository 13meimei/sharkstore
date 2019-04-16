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
namespace server {

static const int kMaxBatchSize = 64;

struct CommonWork {
    uint64_t owner;
    std::atomic<bool>* running;

    std::function<void()> f0;

    void Do();
};

class CommonThread {
public:
    CommonThread(size_t queue_capcity, const std::string& name);
    ~CommonThread();

    CommonThread(const CommonThread&) = delete;
    CommonThread& operator=(const CommonThread&) = delete;

    bool submit(uint64_t owner, std::atomic<bool>* running,
                const std::function<void()>& f0, std::string& cmd);

    bool tryPost(const CommonWork& w);
    void post(const CommonWork& w);
    void waitPost(const CommonWork& w);
    void shutdown();
    int size() const;

private:
    bool pull(CommonWork* w);
    void run();

private:
    const size_t capacity_;

    std::unique_ptr<std::thread> thr_;
    bool running_ = false;
    std::queue<CommonWork> queue_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
};


} /* namespace server */
} /* namespace dataserver */
} /* namespace sharkstore */
