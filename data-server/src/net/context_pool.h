_Pragma("once");

#include <atomic>
#include <thread>
#include <vector>
#include <functional>

#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>

namespace sharkstore {
namespace net {

// 模型：一个线程一个io_context
// GetIOContext()使用轮询返回可用的io_context
class IOContextPool final {
public:
    // size: pool(threads) size
    // name：打印日志和标注线程名
    IOContextPool(size_t size, const std::string& name);
    ~IOContextPool();

    IOContextPool(const IOContextPool&) = delete;
    IOContextPool& operator=(const IOContextPool&) = delete;

    void Start();
    void Stop();

    size_t Size() const { return pool_size_; }

    // must check Size()>0 before
    asio::io_context& GetIOContext();

private:
    void runLoop(const std::shared_ptr<asio::io_context>& ctx, int i);

private:
    using WorkGuard = asio::executor_work_guard<asio::io_context::executor_type>;

    const size_t pool_size_ = 0;
    const std::string pool_name_;

    std::vector<std::shared_ptr<asio::io_context>> io_contexts_;
    std::vector<WorkGuard> work_guards_;
    bool stopped_ = false;

    std::atomic<uint64_t> round_robin_counter_ = {0};

    std::vector<std::thread> threads_;
};

}  // namespace net
}  // namespace sharkstore
