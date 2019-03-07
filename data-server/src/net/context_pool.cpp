#include "context_pool.h"

#include "base/util.h"
#include "frame/sf_logger.h"

namespace sharkstore {
namespace net {

IOContextPool::IOContextPool(size_t size, const std::string& name) :
    pool_size_(size),
    pool_name_(name) {
    for (size_t i = 0; i < size; ++i) {
        auto context = std::make_shared<asio::io_context>();
        auto guard = asio::make_work_guard(*context);
        io_contexts_.push_back(std::move(context));
        work_guards_.push_back(std::move(guard));
    }
}

IOContextPool::~IOContextPool() {
    // stop if running
    Stop();
}

void IOContextPool::Start() {
    for (unsigned i = 0; i < io_contexts_.size(); ++i) {
        std::thread t(std::bind(&IOContextPool::runLoop, this, io_contexts_[i], i));

        char name[16] = {'\0'};
        snprintf(name, 16, "%s-io:%u", pool_name_.c_str(), i);
        AnnotateThread(t.native_handle(), name);

        threads_.push_back(std::move(t));
    }
}

void IOContextPool::Stop() {
    if (stopped_) return;

    stopped_ = true;
    work_guards_.clear();
    for (auto& ctx : io_contexts_) {
        ctx->stop();
    }
    for (auto& t : threads_) {
        t.join();
    }
}

asio::io_context& IOContextPool::GetIOContext() {
    auto idx = round_robin_counter_++ % io_contexts_.size();
    return *(io_contexts_[idx]);
}

void IOContextPool::runLoop(const std::shared_ptr<asio::io_context>& ctx, int i) {
    FLOG_INFO("[Net] %s context pool loop-%d start.", pool_name_.c_str(), i);

    try {
        ctx->run();
    } catch (std::exception& e) {
        FLOG_ERROR("[Net] %s context pool loop-%d run error: %s.", pool_name_.c_str(), i, e.what());
    }

    FLOG_INFO("[Net] %s context pool loop-%d exit.", pool_name_.c_str(), i);
}

}  // namespace net
}  // namespace sharkstore
