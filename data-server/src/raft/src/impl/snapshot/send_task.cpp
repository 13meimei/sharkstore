#include "send_task.h"

namespace sharkstore {
namespace raft {
namespace impl {
namespace snapshot {

SendTask::SendTask(const SnapshotOptions& opt, const SnapContext& ctx,
                   pb::SnapshotMeta& snap_meta,
                   const std::shared_ptr<Snapshot>& snap_data)
    : Task(opt, ctx), snap_meta_(), snap_data_(snap_data) {
    snap_meta_.Swap(&snap_metat);
}

SendTask::~SendTask() {}

Status SendTask::RecvAck(MessagePtr& msg) {
    assert(msg->type() == pb::SNAPSHOT_ACK);
    assert(msg->id() == context_.id);

    // ACK消息跟当前正在发送的快照不一致
    if (msg->snapshot().uuid() != context_.uuid) {
        return Status(Status::kInvalidArgument, "uuid", std::to_string(context_.uuid));
    } else if (msg->term() != context_.term) {
        return Status(Status::kInvalidArgument, "term", std::to_string(context_.term));
    } else if (msg->from() != context_.to) {
        return Status(Status::kInvalidArgument, "from", std::to_string(context_.to));
    }

    auto seq = msg->snapshot().seq();
    auto reject = msg->reject();
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (seq <= ack_seq_) {
            return Status(Status::kInvalidArgument, "stale ack seq",
                          std::to_string(ack_seq_));
        }
        ack_seq_ = seq;
        rejected_ = reject;
    }
    cv_.notify_one();

    return Status::OK();
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

    bool over = false;
    int64_t seq = 0;
    while (!over) {
        if (IsCanceled()) {
            result->status = Status(Status::kAborted, "canceled", "");
            return;
        }

        ++seq;

        // 准备本次数据块
        MessagePtr msg(new pb::Message);
        result->status = nextMsg(seq, msg, over);
        if (!result->status.ok()) {
            return;
        }

        // 发送
        result->status = conn->Send(msg);
        if (!result->status.ok()) {
            return;
        }
        result->blocks_count += 1;
        result->bytes_count += msg->ByteSizeLong();

        // 等待ack
        result->status = waitAck(seq, opt_.ack_timeout_seconds);
        if (!result->status.ok()) {
            return;
        }
    }
    return;
}

Status SendTask::waitAck(int64_t seq, int timeout_secs) {
    std::unique_lock<std::mutex> lock(mu_);
    if (cv_.wait_for(lock, std::chrono::seconds(timeout_secs),
                     [seq, this] { return ack_seq_ >= seq || canceled_ })) {
        if (canceled_) {
            return Status(Status::kAborted, "canceled", "");
        } else if (rejected_) {
            return Status(Status::kAborted, "reject", std::to_string(seq));
        } else {
            return Status::OK();
        }
    } else {
        return Status(Status::kTimedOut, "wait ack", std::to_string(seq));
    }
}

Status SendTask::nextMsg(int64_t seq, MessagePtr& msg, bool* over) {
    msg->set_type(pb::SNAPSHOT_REQUEST);
    msg->set_id(context_.id);
    msg->set_to(context_.to);
    msg->set_from(context_.from);
    msg->set_term(context_.term);

    auto snapshot = msg->mutable_snapshot();
    snapshot->set_uuid(context_.uuid);
    snapshot->set_seq(seq);

    // 第一个数据块，header
    if (seq == 1) {
        snapshot->mutable_meta()->Swap(&snap_meta_);
        *over = false;
        return Status::OK();
    }

    uint64_t size = 0;
    while (!over && size < opt_.max_size_per_msg) {
        if (IsCanceled()) {
            return Status(Status::kAborted, "canceled", "");
        }

        // 拿快照数据
        std::string data;
        auto s = snap_data_->Next(&data, over);
        if (!s.ok()) return s;
        if (data.empty()) continue;

        // 先加后判断，可能会超出一点max_size_per_msg
        // 前提一次Next的数据量不会太大
        size += data.size();
        snapshot->add_datas()->swap(data);
    }

    snapshot->set_final(over);

    return Status::OK();
}

void SendTask::Cancel() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        canceled_ = true;
    }
    cv_.notify_one();
}

} /* snapshot */
} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
