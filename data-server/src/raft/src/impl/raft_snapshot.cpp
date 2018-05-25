#include "raft_snapshot.h"

namespace fbase {
namespace raft {
namespace impl {

std::atomic<uint64_t> SnapshotApplyContext::total_applying(0);

SnapshotRequest::SnapshotRequest() : cancel_(false) {}

SnapshotRequest::~SnapshotRequest() {
    Cancel();
}

void SnapshotRequest::Cancel() {
    // 取消发送并唤醒
    {
        std::lock_guard<std::mutex> lock(mu_);
        cancel_ = true;
    }
    cv_.notify_one();
}

bool SnapshotRequest::Canceled() const {
    std::lock_guard<std::mutex> lock(mu_);
    return cancel_;
}

void SnapshotRequest::Ack(int64_t seq, bool rejected) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        ack_seq_ = seq;
        rejected_ = rejected;
    }
    cv_.notify_one();
}

Status SnapshotRequest::WaitAck(int64_t seq, int wait_secs) {
    std::unique_lock<std::mutex> lock(mu_);
    if(cv_.wait_for(lock, std::chrono::seconds(wait_secs), 
                    [seq, this]{return ack_seq_ >= seq || cancel_;})) {
        if (cancel_) {
            return Status(Status::kAborted, "cancel", std::to_string(header->snapshot().uuid()));
        } else if (rejected_) {
            return Status(Status::kAborted, "remote reject", std::to_string(header->snapshot().uuid()));
        } else {
            return Status::OK();
        }
    } else {
        return Status(Status::kTimedOut, "wait snapshot ack", std::to_string(seq));
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */