#include "snapshot_sender.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "base/util.h"
#include "common/ds_proto.h"

namespace sharkstore {
namespace raft {
namespace impl {

static const int kMaxDataSizePerSnapMsg = 1024;

SnapshotSender::SnapshotSender(transport::Transport *transport,
                               uint8_t concurrency, size_t max_size_per_msg)
    : transport_(transport),
      concurrency_(concurrency),
      max_size_per_msg_(max_size_per_msg),
      running_(true) {}

SnapshotSender::~SnapshotSender() {
    ShutDown();
    for (auto t : send_threads_) {
        delete t;
    }
}

Status SnapshotSender::Start() {
    for (uint8_t i = 0; i < concurrency_; ++i) {
        std::thread *t =
            new std::thread(std::bind(&SnapshotSender::send_loop, this));
        send_threads_.push_back(t);
    }
    return Status::OK();
}

Status SnapshotSender::ShutDown() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (!running_) return Status::OK();
        running_ = false;
    }
    cond_.notify_all();
    for (auto t : send_threads_) {
        if (t->joinable()) {
            t->join();
        }
    }
    return Status::OK();
}

void SnapshotSender::send_loop() {
    while (true) {
        std::shared_ptr<SnapshotRequest> snap;
        {
            std::unique_lock<std::mutex> lock(mu_);
            while (running_ && queue_.empty()) {
                cond_.wait(lock);
            }
            if (!running_) {
                return;
            } else {
                snap = queue_.front();
                queue_.pop();
            }
        }
        // 发送
        SnapshotStatus status;
        do_send(*snap, &status);
        snap->Cancel();
        snap->reporter(snap->header, status);

        {
            std::lock_guard<std::mutex> lock(mu_);
            --sending_count_;
        }
    }
}

void SnapshotSender::Send(std::shared_ptr<SnapshotRequest> snap) {
    bool busy = false;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (sending_count_ >= concurrency_) {
            busy = true;
        } else {
            queue_.push(snap);
            ++sending_count_;
            cond_.notify_all();
        }
    }
    if (busy) {
        SnapshotStatus snap_status;
        snap_status.s =
            Status(Status::kBusy, "max snapshot concurrenct limit reached", "");
        snap->Cancel();
        snap->reporter(snap->header, snap_status);
    }
}

size_t SnapshotSender::GetConcurrency() const {
    std::lock_guard<std::mutex> lock(mu_);
    return sending_count_;
}

void SnapshotSender::send_snapshot(transport::Connection &conn,
                                   SnapshotRequest &snap,
                                   SnapshotStatus *status) {
    static const int kAckTimeoutSec = 10;
    size_t send_bytes = 0;

    // 发送快照头部
    MessagePtr header(new pb::Message);
    header->CopyFrom(*(snap.header));
    status->s = conn.Send(header);
    if (!status->s.ok()) {
        return;
    }
    status->send_bytes += header->ByteSizeLong();

    // 等待头部ack
    status->s = snap.WaitAck(0, kAckTimeoutSec);
    if (!status->s.ok()) return;

    // 发送快照数据块
    MessagePtr msg(new pb::Message);
    msg->CopyFrom(*(snap.header));
    auto snapshot = msg->mutable_snapshot();
    snapshot->clear_meta();  // 快照数据块不需要携带meta信息
    snapshot->set_seq(1);    // 添加快照块序号

    bool over = false;
    uint64_t size = 0;
    std::string data;
    while (running_ && !snap.Canceled() && !over) {
        data.clear();
        status->s = snap.snapshot->Next(&data, &over);
        if (!status->s.ok()) return;
        if (data.empty()) continue;
        size += data.size();
        bool need_switch = (size >= max_size_per_msg_) ||
                           (snapshot->datas_size() >= kMaxDataSizePerSnapMsg);
        if (!need_switch || snapshot->datas_size() == 0) {
            snapshot->add_datas()->swap(data);
        } else {
            snapshot->set_final(false);
            // 发送
            status->s = conn.Send(msg);
            if (!status->s.ok()) return;
            status->send_bytes += msg->ByteSizeLong();
            status->blocks_count++;

            // 等待ack
            status->s = snap.WaitAck(snapshot->seq(), kAckTimeoutSec);
            if (!status->s.ok()) return;

            // reset下一个快照块
            size = 0;
            snapshot->clear_datas();
            snapshot->set_seq(snapshot->seq() + 1);
            snapshot->add_datas()->swap(data);
        }
    }

    // 发送最后一个数据块
    if (running_ && !snap.Canceled()) {
        snapshot->set_final(true);
        status->s = conn.Send(msg);
        if (!status->s.ok()) return;
        status->send_bytes += msg->ByteSizeLong();
        status->blocks_count++;
    }
}

void SnapshotSender::do_send(SnapshotRequest &snap, SnapshotStatus *status) {
    if (!running_ || snap.Canceled()) {
        status->s = Status(Status::kAborted);
        return;
    }

    std::shared_ptr<transport::Connection> conn;
    status->s = transport_->GetConnection(snap.header->to(), &conn);
    if (!status->s.ok()) {
        return;
    }

    assert(conn != nullptr);

    // 发送快照
    this->send_snapshot(*conn, snap, status);

    auto s = conn->Close();
    if (!s.ok() && status->s.ok()) {
        status->s = s;
    }
}

} /* namespace impl */
}  // namespace raft
}  // namespace sharkstore
