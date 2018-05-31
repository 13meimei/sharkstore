#include "send_task.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

SendTask(const SnapContext& context, const SnapReporter& reporter,
         pb::SnapshotMeta& snap_meta, const std::shared_ptr<Snapshot>& snap_data)
    : Task(context, reporter), snap_meta_(), snap_data_(snap_data) {
    snap_meta_.Swap(&snap_meta);
}

SendTask::~SendTask() {}

void SendTask::RecvAck(int64_t seq, bool rejected) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        ack_seq_ = seq;
        rejected_ = rejected;
    }
    cv_.notify_one();
}

void SendTask::Run(SnapResult* result) {
    assert(transport_ != nullptr);

    if (IsCanceled()) {
        result->status = Status(Status::kAborted);
        return;
    }

    // 建立链接
    std::shared_ptr<transport::Connection> conn;
    status->s = transport_->GetConnection(snap.header->to(), &conn);
    if (!status->s.ok()) {
        return;
    }
}

Status SendTask::waitAck(int64_t seq, int timeout_secs) {
    std::unique_lock<std::mutex> lock(mu_);
    if (cv_.wait_for(lock, std::chrono::seconds(timeout_secs),
                     [seq, this] { return ack_seq_ >= seq || canceled_ })) {
        if (canceled_) {
            return Status(Status::kAborted);
        } else if (rejected_) {
            return Status(Status::kAborted, "reject", std::to_string(seq));
        } else {
            return Status::OK();
        }
    } else {
        return Status(Status::kTimedOut, "wait ack", std::to_string(seq));
    }
}

std::tuple<MessagePtr, bool> nextMsg(int64_t seq, uint64_t max_size) {
    // seq = 0，发送快照头部
    if (seq == 0) {
    }
}

void SendTask::Cancel() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        canceled_ = true;
    }
    cv_.notify_one();
}

bool SendTask::IsCanceled() const {
    std::lock_guard<std::mutex> lock(mu_);
    return canceled_;
}

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
