#include "worker.h"

#include "task.h"
#include "worker_pool.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

Worker::Worker(WorkerPool* pool) : pool_(pool) {
    thr_ = std::thread([this] { runTask(); });
}

Worker::~Worker() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        running_ = false;
        if (task_ != nullptr) {
            task_->Cancel();
        }
    }
    cv_.notify_one();
    thr_.join();
}

void Worker::post(const std::shared_ptr<Task>& task) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        assert(task_ == nullptr);
        task_ = task;
    }
    cv_.notify_one();
}

void Worker::runTask() {
    std::shared_ptr<Task> task;
    {
        std::unique_lock<std::mutex> lock(mu_);
        while (task_ == nullptr && running_) {
            cv_.wait(lock);
        }
        if (!running_) return;
        task = std::move(task_);
    }

    SnapResult result;
    task->Run(&result);
    task->Report(result);

    pool_->addToFreeList(this);
}

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
