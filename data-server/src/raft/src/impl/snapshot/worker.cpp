#include "worker.h"

#include "base/util.h"

#include "task.h"
#include "worker_pool.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

Worker::Worker(WorkerPool* pool, const std::string& name) : pool_(pool) {
    thr_ = std::thread([this] { runTask(); });
    AnnotateThread(thr_.native_handle(), name.c_str());
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

void Worker::post(const std::shared_ptr<SnapTask>& task) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        assert(task_ == nullptr);
        task_ = task;
    }
    cv_.notify_one();
}

void Worker::runTask() {
    std::shared_ptr<SnapTask> task;
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
