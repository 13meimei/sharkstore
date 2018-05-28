#include "raft_impl.h"

#include <sstream>
#include "logger.h"
#include "raft_exception.h"
#include "raft_fsm.h"
#include "raft_types.h"
#include "storage/storage.h"

namespace fbase {
namespace raft {
namespace impl {

static const uint64_t kMaxSnapSizePerMsg = 1024 * 1024 * 1;
static const time_t kFetchStatusIntervalSec = 3;

RaftImpl::RaftImpl(const RaftServerOptions& sops, const RaftOptions& ops,
                   const RaftContext& ctx)
    : sops_(sops), ops_(ops), ctx_(ctx), fsm_(new RaftFsm(sops, ops)) {}

RaftImpl::~RaftImpl() { Stop(); }

Status RaftImpl::TryToLeader() {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }
    // 发送选举消息
    MessagePtr msg(new pb::Message);
    msg->set_type(pb::LOCAL_MSG_HUP);
    msg->set_from(sops_.node_id);
    RecvMsg(msg);
    return Status::OK();
}

void RaftImpl::post(const std::function<void()>& f) {
    Work w;
    w.owner = ops_.id;
    w.stopped = &stopped_;
    w.f0 = f;
    ctx_.consensus_thread->post(w);
}

bool RaftImpl::tryPost(const std::function<void()>& f) {
    Work w;
    w.owner = ops_.id;
    w.stopped = &stopped_;
    w.f0 = f;
    return ctx_.consensus_thread->tryPost(w);
}

Status RaftImpl::Submit(std::string& cmd) {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }

    if (ctx_.consensus_thread->submit(
            ops_.id, &stopped_,
            std::bind(&RaftImpl::Step, shared_from_this(), std::placeholders::_1), cmd)) {
        return Status::OK();
    } else {
        return Status(Status::kBusy);
    }
}

Status RaftImpl::ChangeMemeber(const ConfChange& conf) {
    if (stopped_) {
        return Status(Status::kShutdownInProgress, "raft is removed",
                      std::to_string(ops_.id));
    }

    std::string str;
    auto s = EncodeConfChange(conf, &str);
    if (!s.ok()) return s;

    auto msg = std::make_shared<pb::Message>();
    msg->set_type(pb::LOCAL_MSG_PROP);
    auto entry = msg->add_entries();
    entry->set_type(pb::ENTRY_CONF_CHANGE);
    entry->mutable_data()->swap(str);

    if (tryPost(std::bind(&RaftImpl::Step, shared_from_this(), msg))) {
        return Status::OK();
    } else {
        return Status(Status::kBusy);
    }

    return Status::OK();
}

void RaftImpl::Truncate(uint64_t index) {
    post(std::bind(&RaftImpl::truncate, shared_from_this(), index));
}

void RaftImpl::RecvMsg(MessagePtr& msg) {
#ifdef FBASE_RAFT_TRACE_MSG
    if (msg->type() != pb::LOCAL_TICK) {
        LOG_DEBUG("recv msg type: %s from %llu, term: %llu at term: %llu",
                  pb::MessageType_Name(msg->type()).c_str(), msg->from(), msg->term(),
                  fsm_->term_);
    } else {
        LOG_DEBUG("recv msg type: LOCAL_TICK messages term: %llu at term: %llu",
                  msg->term(), fsm_->term_);
    }
#endif
    if (stopped_) return;

    // TODO: remove this
    if (msg->type() == pb::SNAPSHOT_RESPONSE) {  // 快照结果不丢弃
        post(std::bind(&RaftImpl::Step, shared_from_this(), msg));
    } else {
        if (!tryPost(std::bind(&RaftImpl::Step, shared_from_this(), msg))) {
            LOG_DEBUG("raft[%llu] discard a msg. type: %s from %llu, term: %llu", ops_.id,
                      pb::MessageType_Name(msg->type()).c_str(), msg->from(),
                      msg->term());
        }
    }
}

void RaftImpl::Step(MessagePtr& msg) {
    if (!fsm_->Validate(msg)) {
        LOG_DEBUG("raft[%lu] ignore invalidate msg type: %s from %llu, term: %llu",
                  ops_.id, pb::MessageType_Name(msg->type()).c_str(), msg->from(),
                  msg->term());
        return;
    }

    fsm_->Step(msg);
    fsm_->GetReady(&ready_);

    // 发送
    send();

    // 应用
    apply();

    // 发布状态更新
    publish();

    // 持久化
    persist();
}

void RaftImpl::send() {
    if (!ready_.msgs.empty()) {
        for (auto m : ready_.msgs) {
            ctx_.msg_sender->SendMessage(m);
        }
    }
    if (ready_.snapshot) {
        auto snap = ready_.snapshot;
        snap->reporter = std::bind(&RaftImpl::ReportSnapshotStatus, shared_from_this(),
                                   std::placeholders::_1, std::placeholders::_2);
        snap->sending = true;
        ctx_.snap_sender->Send(snap);
    }
}

// 应用
void RaftImpl::apply() {
    const auto& ents = ready_.committed_entries;
    for (const auto& e : ents) {
        if (e->type() == pb::ENTRY_CONF_CHANGE) {
            auto s = fsm_->applyConfChange(e);
            if (!s.ok()) {
                throw RaftException(std::string("apply confchange[") +
                                    std::to_string(e->index()) + "] error: " +
                                    s.ToString());
            }
            conf_changed_ = true;
        }
        if (sops_.apply_in_place) {
            // 同步应用
            smApply(e);
        } else {
            // 异步应用
            assert(ctx_.apply_thread != nullptr);
            Work w;
            w.owner = ops_.id;
            w.stopped = &stopped_;
            w.f0 = std::bind(&RaftImpl::smApply, shared_from_this(), e);
            ctx_.apply_thread->waitPost(w);
        }
    }
    if (!ents.empty()) {
        fsm_->raft_log_->appliedTo(fsm_->raft_log_->committed());
    }
}

// 持久化
void RaftImpl::persist() {
    auto hs = fsm_->GetHardState();
    bool hs_changed = prev_hard_state_.term() != hs.term() ||
                      prev_hard_state_.vote() != hs.vote() ||
                      prev_hard_state_.commit() != hs.commit();
    if (hs_changed) {
        prev_hard_state_ = hs;
    }
    auto s = fsm_->Persist(hs_changed);
    if (!s.ok()) throw RaftException(s);
}

void RaftImpl::publish() {
    uint64_t leader = 0, term = 0;
    std::tie(leader, term) = fsm_->GetLeaderTerm();
    if (leader != bulletin_board_.Leader() || term != bulletin_board_.Term()) {
        bulletin_board_.PublishLeaderTerm(leader, term);
        ops_.statemachine->OnLeaderChange(leader, term);
    }

    if (conf_changed_) {
        conf_changed_ = false;
        bulletin_board_.PublishPeers(fsm_->GetPeers());
    }

    // 定时更新status
    time_t now = time(NULL);
    if (now > last_fetch_status_ && now - last_fetch_status_ > kFetchStatusIntervalSec) {
        last_fetch_status_ = now;
        bulletin_board_.PublishStatus(fsm_->GetStatus());
    }
}

void RaftImpl::ReportSnapshotStatus(const MessagePtr& header, const SnapshotStatus& ss) {
    if (ss.s.ok()) {
        LOG_INFO("raft[%llu] send snapshot[uuid: %lu] to %lu finished. total "
                 "blocks: %d, bytes: %d",
                 ops_.id, header->snapshot().uuid(), header->to(), ss.blocks_count,
                 ss.send_bytes);
    } else {
        LOG_ERROR("raft[%llu] send snapshot[uuid: %llu] to %lu failed(%s). "
                  "sent blocks: %d, bytes: %d",
                  ops_.id, header->snapshot().uuid(), header->to(),
                  ss.s.ToString().c_str(), ss.blocks_count, ss.send_bytes);
    }

    // 通知本地leader
    MessagePtr resp(new pb::Message);
    resp->set_type(pb::SNAPSHOT_RESPONSE);
    resp->set_to(header->from());
    resp->set_from(header->to());
    resp->set_term(header->term());
    resp->set_reject(!ss.s.ok());
    resp->mutable_snapshot()->set_uuid(header->snapshot().uuid());
    RecvMsg(resp);
}

void RaftImpl::smApply(const EntryPtr& e) {
    auto s = fsm_->smApply(e);
    if (!s.ok()) {
        throw RaftException(std::string("statemachine apply entry[") +
                            std::to_string(e->index()) + "] error: " + s.ToString());
    }
}

void RaftImpl::Stop() { stopped_ = true; }

void RaftImpl::truncate(uint64_t index) {
    LOG_DEBUG("raft[%llu] truncate %llu", ops_.id, index);
    fsm_->TruncateLog(index);
}

Status RaftImpl::BackupLog() { return fsm_->BackupLog(); }

Status RaftImpl::Destroy() { return fsm_->DestroyLog(); }

}  // namespace impl
}  // namespace raft
} /* namespace fbase */
