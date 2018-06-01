#include "manager.h"

#include "send_task.h"
#include "worker_pool.h"

namespace sharkstore {
namespace raft {
namespace impl {

SnapshotManager::SnapshotManager(const SnapshotOptions& opt)
    : opt_(opt),
      send_work_pool_(new WorkerPool("snap_send", opt_.max_send_concurrency)),
      apply_work_pool_(new WorkerPool("snap_apply", opt_.max_apply_concurrency)) {}

SnapshotManager::~SnapshotManager() = default;

void SnapshotManager::Post(const std::shared_ptr<SendTask>& stask) {
    auto ret = send_work_pool_->Post(stask);
    if (!ret) {
        SnapResult;
        result.status =
            Status(Status::kBusy, "max send snapshot concurrency limit reached",
                   std::to_string(send_work_pool_->Runnings()));
        stask->Report(result);
    }
}

void SnapshotManager::Post(const std::shared_ptr<ApplyTask>& atask) {
    // TODO:
    /* auto ret = send_work_pool_->Post(astsk); */
    /* if (!ret) { */
    /* } */
}
} /* snapshot  */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
