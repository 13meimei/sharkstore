#include "raft_fsm.h"

#include <algorithm>
#include <sstream>
#include "logger.h"
#include "raft_exception.h"

namespace fbase {
namespace raft {
namespace impl {

static const int kDownPeerThresholdSecs = 50;

void RaftFsm::becomeLeader() {
    if (state_ == FsmState::kFollower) {
        throw RaftException(
            "[raft->becomeLeader]invalid transition [follower -> leader].");
    }

    uint64_t lasti = raft_log_->lastIndex();
    step_func_ = std::bind(&RaftFsm::stepLeader, this, std::placeholders::_1);
    reset(term_, lasti, true);
    tick_func_ = std::bind(&RaftFsm::tickHeartbeat, this);
    leader_ = node_id_;
    state_ = FsmState::kLeader;

    std::vector<EntryPtr> ents;
    Status s = raft_log_->entries(raft_log_->committed() + 1, kNoLimit, &ents);
    if (!s.ok()) {
        throw RaftException(std::string("[raft->becomeLeader] unexpected error "
                                        "getting uncommitted entries:") +
                            s.ToString());
    }
    int nconf = numOfPendingConf(ents);
    if (nconf > 1) {
        throw RaftException(
            "[raft->becomeLeader]unexpected double uncommitted config "
            "entry");
    } else if (nconf == 1) {
        pending_conf_ = true;
    }

    // 添加一条空日志
    // 1) 像其他follower通知新leader
    // 2) 尝试提交前任leader未提交的日志
    EntryPtr entry(new pb::Entry);
    entry->set_type(pb::ENTRY_NORMAL);
    entry->set_term(term_);
    entry->set_index(lasti + 1);
    appendEntry(std::vector<EntryPtr>{entry});

    LOG_INFO("raft[%llu] become leader at term %llu", id_, term_);
}

void RaftFsm::stepLeader(MessagePtr& msg) {
    if (msg->type() == pb::LOCAL_MSG_PROP) {
        if (replicas_.find(node_id_) != replicas_.end() &&
            msg->entries_size() > 0) {
            std::vector<EntryPtr> ents;
            takeEntries(msg, ents);
            // 保证同时只有一个ConfChange
            auto li = raft_log_->lastIndex();
            for (auto& entry : ents) {
                entry->set_index(++li);
                entry->set_term(term_);
                if (entry->type() == pb::ENTRY_CONF_CHANGE) {
                    if (pending_conf_) {
                        // 重新为空日志
                        entry->set_type(pb::ENTRY_NORMAL);
                        entry->clear_data();
                    }
                    pending_conf_ = true;
                }
            }
            appendEntry(ents);
            bcastAppend();
        }
        return;
    }

    // 其他类型的消息需要读取副本的进度
    auto it = replicas_.find(msg->from());
    if (it == replicas_.end()) {
        LOG_WARN("raft[%llu] stepLeader no progress available for %llu", id_,
                 msg->from());
        return;
    }
    Replica& pr = *(it->second);

    switch (msg->type()) {
        case pb::APPEND_ENTRIES_RESPONSE:
            pr.set_active();

            if (msg->reject()) {
                LOG_DEBUG("raft[%llu] received msgApp "
                          "rejection(lastindex:%llu) from %llu for index %llu",
                          id_, msg->reject_hint(), msg->from(),
                          msg->log_index());

                if (pr.maybeDecrTo(msg->log_index(), msg->reject_hint(),
                                   msg->commit())) {
                    if (pr.state() == ReplicaState::kReplicate) {
                        pr.becomeProbe();
                    }
                    sendAppend(msg->from());
                }
            } else {
                bool old_paused = pr.isPaused();
                if (pr.maybeUpdate(msg->log_index(), msg->commit())) {
                    switch (pr.state()) {
                        case ReplicaState::kProbe:
                            pr.becomeReplicate();
                            break;
                        case ReplicaState::kReplicate:
                            pr.inflight().freeTo(msg->log_index());
                            break;
                        case ReplicaState::kSnapshot:
                            if (pr.needSnapshotAbort()) {
                                LOG_INFO("raft[%llu] snapshot aborted, resumed "
                                         "sending replication to %llu",
                                         id_, msg->from());
                                pr.becomeProbe();
                            }
                            break;
                    }
                    if (maybeCommit()) {
                        bcastAppend();  // commit位置有更新，通知followers
                    } else if (old_paused) {
                        sendAppend(msg->from());
                    }
                }
            }
            return;

        case pb::HEARTBEAT_RESPONSE:
            pr.set_active();
            if (pr.state() == ReplicaState::kReplicate &&
                pr.inflight().full()) {
                pr.inflight().freeFirstOne();
            }
            // 进度没跟上，需要复制
            if (!pr.pending() && (pr.match() < raft_log_->lastIndex() ||
                                  pr.committed() < raft_log_->committed())) {
                sendAppend(msg->from());
            }
            if (pr.state() != ReplicaState::kSnapshot) {
                pr.set_pending(false);
            }
            return;

        case pb::SNAPSHOT_ACK:
            if (pending_send_snap_ &&
                pending_send_snap_->header->snapshot().uuid() ==
                    msg->snapshot().uuid()) {
                LOG_DEBUG(
                    "raft[%lu] recv snapshot[%lu] ack. seq=%ld, reject=%d", id_,
                    msg->snapshot().uuid(), msg->snapshot().seq(),
                    msg->reject());
                pending_send_snap_->Ack(msg->snapshot().seq(), msg->reject());
            }
            return;

        case pb::SNAPSHOT_RESPONSE:
            if (pr.state() != ReplicaState::kSnapshot) {
                return;
            }

            resetSnapshotSend();

            if (msg->reject()) {
                LOG_WARN("raft[%llu] send snapshot to [%llu] failed(rejected).",
                         id_, msg->from());
                pr.snapshotFailure();
                pr.becomeProbe();
            } else {
                pr.set_active();
                pr.becomeProbe();
                LOG_WARN("raft[%llu] send snapshot to [%llu] succeed, resumed "
                         "replication [%s]",
                         id_, msg->from(), pr.ToString().c_str());
            }

            pr.pause();
            return;
        default:
            return;
    }
}

void RaftFsm::tickHeartbeat() {
    ++heartbeat_elapsed_;
    if (state_ != FsmState::kLeader) {
        return;
    }
    if (heartbeat_elapsed_ >= sops_.heartbeat_tick) {
        heartbeat_elapsed_ = 0;
        for (auto& r : replicas_) {
            if (r.first == node_id_) {
                continue;
            }
            if (r.second->state() != ReplicaState::kSnapshot) {
                r.second->resume();
            }
        }
        // 定时检查follwer的健康状态
        checkDownPeers();
        checkPendingPeers();

        // 检查释放可能已cancel的正在发送的快照
        checkSnapSend();
    }
}

bool RaftFsm::maybeCommit() {
    std::vector<uint64_t> matches;
    matches.reserve(replicas_.size());
    for (const auto& r : replicas_) {
        matches.push_back(r.second->match());
    }
    std::sort(matches.begin(), matches.end(), std::greater<uint64_t>());

// debug模式下打印落后进度
#ifndef NDEBUG
    if (matches.size() >= 3 &&
        (matches[matches.size() - 2] - matches[matches.size() - 1] > 5000)) {
        std::string tmp;
        for (const auto& r : replicas_) {
            tmp += "(";
            tmp += std::to_string(r.first);
            tmp += ",";
            tmp += std::to_string(r.second->match());
            tmp += ")";
            tmp += "-";
        }
        LOG_DEBUG("raft[%llu] commit pos: %s", id_, tmp.c_str());
    }
#endif

    uint64_t mid = matches[quorum() - 1];
    bool is_commit = raft_log_->maybeCommit(mid, term_);
    if (state_ == FsmState::kLeader) {
        auto it = replicas_.find(node_id_);
        if (it != replicas_.end()) {
            it->second->set_committed(raft_log_->committed());
        }
    }
    return is_commit;
}

void RaftFsm::bcastAppend() {
    for (const auto& r : replicas_) {
        if (r.first == node_id_) {
            continue;
        }
        sendAppend(r.first);
    }
}

void RaftFsm::sendAppend(uint64_t to) {
    auto it = replicas_.find(to);
    assert(it != replicas_.end());
    Replica& pr = *(it->second);
    if (pr.isPaused()) {
        return;
    }

    Status ts, es;
    uint64_t term = 0;
    std::vector<EntryPtr> ents;

    uint64_t fi = raft_log_->firstIndex();
    if (pr.next() >= fi) {
        ts = raft_log_->term(pr.next() - 1, &term);
        es = raft_log_->entries(pr.next(), sops_.max_size_per_msg, &ents);
    }

    // 需要发快照
    if (pr.next() < fi || !ts.ok() || !es.ok()) {
        LOG_INFO("raft[%llu] need snapshot to %llu[next:%llu], fi:%llu, log "
                 "error:%s-%s",
                 id_, to, pr.next(), fi, ts.ToString().c_str(),
                 es.ToString().c_str());

        if (!pr.active()) {
            LOG_DEBUG("raft[%llu] sendAppend ignore sending snapshot to %llu "
                      "since it is not recently active.",
                      id_, to);
            return;
        }

        if (pending_send_snap_) {
            LOG_WARN(
                "raft[%llu] sendAppend could not send snapshot to %llu(other "
                "snapshot[%lu] is sending)",
                id_, to, pending_send_snap_->header->snapshot().uuid());
            return;
        }

        pending_send_snap_.reset(new SnapshotRequest);
        createSnapshot(to, pending_send_snap_.get());
        auto snap_index = pending_send_snap_->header->snapshot().meta().index();
        auto snap_term = pending_send_snap_->header->snapshot().meta().term();
        pr.becomeSnapshot(snap_index);

        LOG_DEBUG("raft[%llu] sendAppend [firstindex: %llu, commit: %llu] sent "
                  "snapshot[index: %llu, term: %llu] to [%llu][%s]",
                  id_, raft_log_->firstIndex(), raft_log_->committed(),
                  snap_index, snap_term, to, pr.ToString().c_str());

        pr.set_pending(true);
    } else {
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::APPEND_ENTRIES_REQUEST);
        msg->set_to(to);
        msg->set_log_index(pr.next() - 1);  // prev log index
        msg->set_log_term(term);            // prev log term
        msg->set_commit(raft_log_->committed());
        putEntries(msg, ents);

        if (msg->entries_size() > 0) {
            switch (pr.state()) {
                case ReplicaState::kReplicate: {
                    uint64_t last =
                        msg->entries(msg->entries_size() - 1).index();
                    pr.update(last);
                    pr.inflight().add(last);
                    break;
                }
                case ReplicaState::kProbe:
                    pr.pause();
                    break;
                case ReplicaState::kSnapshot:
                    throw RaftException(
                        std::string("[repl->sendAppend][%v] is sending append "
                                    "in unhandled state ") +
                        ReplicateStateName(pr.state()));
            }
        }
        pr.set_pending(true);
        send(msg);
    }
}

void RaftFsm::appendEntry(const std::vector<EntryPtr>& ents) {
    LOG_DEBUG("raft[%llu] append log entry. index: %llu, size: %d", id_,
              ents[0]->index(), ents.size());

    raft_log_->append(ents);
    replicas_[node_id_]->maybeUpdate(raft_log_->lastIndex(),
                                     raft_log_->committed());
    maybeCommit();
}

void RaftFsm::createSnapshot(uint64_t to, SnapshotRequest* snap) {
    auto snapshot = sm_->GetSnapshot();
    if (snapshot == nullptr) {
        throw RaftException("raft->sendAppend failed to send snapshot, because "
                            "snapshot is unavailable");
    } else if (snapshot->ApplyIndex() < raft_log_->firstIndex() - 1) {
        std::ostringstream ss;
        ss << "raft->sendAppend[" << id_
           << "]failed to send snapshot, because snapshot is invalid(apply="
           << snapshot->ApplyIndex() << ", first=" << raft_log_->firstIndex()
           << ").";
        throw RaftException(ss.str());
    }

    MessagePtr msg(new pb::Message);
    msg->set_type(pb::SNAPSHOT_REQUEST);
    msg->set_id(id_);
    msg->set_from(node_id_);
    msg->set_to(to);
    msg->set_term(term_);

    snap->header = msg;
    snap->snapshot = snapshot;

    // set snapshot uuid
    auto now = std::chrono::system_clock::now();
    auto uuid = std::chrono::time_point_cast<std::chrono::nanoseconds>(now)
                    .time_since_epoch()
                    .count();
    msg->mutable_snapshot()->set_uuid(uuid);

    auto meta = msg->mutable_snapshot()->mutable_meta();

    // set meta index
    meta->set_index(snapshot->ApplyIndex());

    // set meta term
    uint64_t snap_term = 0;
    auto status = raft_log_->term(snapshot->ApplyIndex(), &snap_term);
    if (!status.ok()) {
        std::ostringstream ss;
        ss << "[raft->sendAppend][" << id_ << "] failed to send snapshot to "
           << to << " because snapshot is unavailable, error is: "
           << status.ToString();
        throw RaftException(ss.str());
    } else {
        meta->set_term(snap_term);
    }

    // set meta peers
    for (const auto& r : replicas_) {
        auto p = meta->add_peers();
        p->CopyFrom(r.second->peer());
    }

    // set use context
    std::string context;
    auto s = snapshot->Context(&context);
    if (!s.ok()) {
        throw RaftException(std::string("get snapshot user context failed: ") +
                            s.ToString());
    }
    meta->set_context(std::move(context));
}

void RaftFsm::checkDownPeers() {
    down_peers_.clear();
    if (leader_ == node_id_) {
        for (const auto& p : replicas_) {
            if (p.first == node_id_) continue;
            if (p.second->inactive_seconds() > kDownPeerThresholdSecs) {
                down_peers_.push_back(DownPeer());
                down_peers_.back().node_id = p.first;
                down_peers_.back().down_seconds = p.second->inactive_seconds();
                LOG_DEBUG("raft[%lu] down peer checked[node=%lu, secs=%d]", id_,
                          p.first, p.second->inactive_seconds());
            }
        }
    }
}

void RaftFsm::checkPendingPeers() {
    pending_peers_.clear();
    if (leader_ == node_id_) {
        for (const auto& p : replicas_) {
            if (p.first == node_id_) continue;
            if (p.second->state() == ReplicaState::kSnapshot) {
                pending_peers_.push_back(p.first);
            }
        }
    }
}

void RaftFsm::checkSnapSend() {
    if (pending_send_snap_ && pending_send_snap_->Canceled()) {
        pending_send_snap_.reset();
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
