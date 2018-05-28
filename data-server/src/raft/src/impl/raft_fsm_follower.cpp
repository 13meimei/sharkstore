#include "raft_fsm.h"
#include "logger.h"
#include "storage/storage.h"

namespace fbase {
namespace raft {
namespace impl {

void RaftFsm::becomeFollower(uint64_t term, uint64_t leader) {
    step_func_ = std::bind(&RaftFsm::stepFollower, this, std::placeholders::_1);
    reset(term, false);
    tick_func_ = std::bind(&RaftFsm::tickElection, this);
    leader_ = leader;
    state_ = FsmState::kFollower;

    LOG_INFO("raft[%llu] become follwer at term %llu", id_, term_);
}

void RaftFsm::stepFollower(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_PROP:
            if (leader_ == 0) {
                LOG_DEBUG("raft[%llu] no leader at term %llu; dropping proposal.", id_,
                          term_);
            } else {
                // 转发给leader
                msg->set_to(leader_);
                send(msg);
            }
            return;

        case pb::APPEND_ENTRIES_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            handleAppendEntries(msg);
            return;

        case pb::HEARTBEAT_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            return;

        case pb::SNAPSHOT_REQUEST:
            election_elapsed_ = 0;
            leader_ = msg->from();
            handleSnapshot(msg);
            return;

        default:
            return;
    }
}

void RaftFsm::tickElection() {
    // 检查是否还在成员内
    if (!electable()) {
        election_elapsed_ = 0;
        return;
    }

    ++election_elapsed_;
    // 选举超时
    if (pastElectionTimeout()) {
        election_elapsed_ = 0;
        MessagePtr msg(new pb::Message);
        msg->set_type(pb::LOCAL_MSG_HUP);
        msg->set_from(node_id_);
        Step(msg);
    }
}

void RaftFsm::handleAppendEntries(MessagePtr& msg) {
    MessagePtr resp_msg(new pb::Message);
    resp_msg->set_type(pb::APPEND_ENTRIES_RESPONSE);
    resp_msg->set_to(msg->from());
    resp_msg->set_reject(false);

    // 复制的日志不能小于commit位置，如果小于告诉leader日志已经到了commit的位置，
    // 请发送大于commit位置的日志
    if (msg->log_index() < raft_log_->committed()) {
        resp_msg->set_log_index(raft_log_->committed());
        resp_msg->set_commit(raft_log_->committed());
        send(resp_msg);
        return;
    }

    std::vector<EntryPtr> ents;
    takeEntries(msg, ents);
    uint64_t last_index = 0;
    if (raft_log_->maybeAppend(msg->log_index(), msg->log_term(), msg->commit(), ents,
                               &last_index)) {
        resp_msg->set_log_index(last_index);
        resp_msg->set_commit(raft_log_->committed());
        send(resp_msg);
    } else {
        LOG_DEBUG("raft[%llu] [logterm:%llu, index:%llu] rejected msgApp from "
                  "%llu[logterm:%llu, index:%llu]",
                  id_, raft_log_->lastTerm(), raft_log_->lastIndex(), msg->from(),
                  msg->log_term(), msg->log_index());

        resp_msg->set_log_index(msg->log_index());
        resp_msg->set_commit(raft_log_->committed());
        resp_msg->set_reject(true);
        resp_msg->set_reject_hint(raft_log_->lastIndex());
        send(resp_msg);
    }
}

void RaftFsm::handleSnapshot(MessagePtr& msg) {
    bool over = false;
    Status status = applySnapshot(msg, &over);
    if (!status.ok()) {
        // 应用快照失败
        LOG_ERROR("raft[%llu] apply snapshot[%llu] from %llu failed: %s", id_,
                  msg->snapshot().uuid(), msg->from(), status.ToString().c_str());

        // 重置SnapshotApplyContext
        applying_snap_.reset();

        // 给远程leader发送失败消息
        MessagePtr resp(new pb::Message);
        resp->set_type(pb::SNAPSHOT_RESPONSE);
        resp->set_to(msg->from());
        resp->set_reject(true);
        send(resp);
        return;
    } else if (over) {
        // 应用快照完成，重置SnapshotApplyContext
        applying_snap_.reset();
    }

    // 给远程发送快照ack
    MessagePtr ack(new pb::Message);
    ack->set_type(pb::SNAPSHOT_ACK);
    ack->set_to(msg->from());
    // 正常情况下，对方发送最后一个快照数据的时候不需要接收ack
    // over也reject是为了兼容以下情况：
    // 快照过期不用应用(此时over为true， status ok），则需要reject，让对方别发了
    ack->set_reject(!status.ok() || over);
    ack->mutable_snapshot()->set_uuid(msg->snapshot().uuid());
    ack->mutable_snapshot()->set_seq(msg->snapshot().seq());

    LOG_DEBUG("raft[%lu] send snapshot[%lu] ack to %lu. seq=%ld, reject=%d", id_,
              ack->snapshot().uuid(), ack->to(), ack->snapshot().seq(), ack->reject());

    send(ack);
}

Status RaftFsm::applySnapshot(MessagePtr& msg, bool* over) {
    bool first = false;
    const auto& snapshot = msg->snapshot();

    // 当前没有快照，或者一个新的uuid不同的快照过来。即当前msg是新快照的第一个消息
    if (!applying_snap_ || applying_snap_->uuid != snapshot.uuid()) {
        if (snapshot.uuid() == 0) {
            // uuid不能为0
            return Status(Status::kInvalidArgument, "invalid snapshot uuid", "0");
        } else if (!snapshot.has_meta()) {
            // 第一条快照消息必须包含meta信息
            return Status(Status::kInvalidArgument, "invalid snapshot meta", "NULL");
        } else if (snapshot.seq() != 0) {
            // 第一条快照消息的序号应该为0
            return Status(Status::kInvalidArgument, "invalid snapshot no-zero seq",
                          std::to_string(snapshot.seq()));
        } else {  // a new apply context
            if (applying_snap_ != nullptr) {
                LOG_WARN("raft[%lu] [logterm: %lu, index: %lu] abort apply "
                         "prev snapshot[%lu] at term %lu.",
                         id_, raft_log_->lastTerm(), raft_log_->lastIndex(),
                         applying_snap_->uuid, term_);
            }

            applying_snap_.reset(new SnapshotApplyContext);
            applying_snap_->uuid = snapshot.uuid();
            applying_snap_->meta = snapshot.meta();
            first = true;
        }
    } else if (snapshot.seq() != applying_snap_->prev_seq + 1) {
        // 快照序号不连续
        return Status(Status::kInvalidArgument, "discontinuous snapshot seq",
                      std::string("prev:") + std::to_string(applying_snap_->prev_seq) +
                          ", msg:" + std::to_string(snapshot.seq()));
    } else {
        // 更新序列号
        applying_snap_->prev_seq = snapshot.seq();
    }

    // 第一条快照消息
    if (first) {
        const auto& meta = snapshot.meta();
        // 检查meta信息
        if (!checkSnapshot(meta)) {
            LOG_WARN("raft[%llu] [commit: %llu] ignored snapshot [index: %llu, "
                     "term: %llu].",
                     id_, raft_log_->committed(), meta.index(), meta.term());

            MessagePtr resp(new pb::Message);
            resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
            resp->set_to(msg->from());
            resp->set_log_index(raft_log_->committed());  // TODO:?
            resp->set_commit(raft_log_->committed());
            send(resp);
            *over = true;
            return Status::OK();
        } else {
            LOG_WARN("raft[%llu] [commit: %llu] start apply snapshot [uuid: "
                     "%llu, index: %llu, term: %llu] from %lu, total applying: %lu",
                     id_, raft_log_->committed(), snapshot.uuid(), meta.index(),
                     meta.term(), msg->from(),
                     SnapshotApplyContext::total_applying.load());

            // 应用快照前，先清空raft日志
            storage_->ApplySnapshot(pb::SnapshotMeta());
            auto status = sm_->ApplySnapshotStart(meta.context());
            if (!status.ok()) {
                return status;
            }
        }
    }

    // 应用快照数据
    if (snapshot.datas_size() > 0) {
        std::vector<std::string> datas;
        datas.resize(snapshot.datas_size());
        for (int i = 0; i < snapshot.datas_size(); ++i) {
            msg->mutable_snapshot()->mutable_datas(i)->swap(datas[i]);
        }
        auto status = sm_->ApplySnapshotData(datas);
        if (!status.ok()) {
            return status;
        }
    }

    if (snapshot.final()) {
        LOG_WARN("raft[%llu] [commit: %llu] apply snapshot [uuid: %llu] from "
                 "%lu finished, total blocks: %ld",
                 id_, raft_log_->committed(), snapshot.uuid(), msg->from(),
                 snapshot.seq());

        auto status = sm_->ApplySnapshotFinish(applying_snap_->meta.index());
        if (!status.ok()) {
            return status;
        }

        status = storage_->ApplySnapshot(applying_snap_->meta);
        if (!status.ok()) return status;

        status = restore(applying_snap_->meta);
        if (!status.ok()) return status;

        *over = true;

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
        resp->set_to(msg->from());
        resp->set_log_index(raft_log_->lastIndex());
        resp->set_commit(raft_log_->committed());
        send(resp);
    }

    return Status::OK();
}

bool RaftFsm::checkSnapshot(const pb::SnapshotMeta& meta) {
    if (meta.index() <= raft_log_->committed()) {
        // 快照的数据都已经commit过了
        return false;
    } else if (raft_log_->matchTerm(meta.index(), meta.term())) {
        // 快照的数据日志里都有
        raft_log_->commitTo(meta.index());
        return false;
    } else {
        return true;
    }
}

Status RaftFsm::restore(const pb::SnapshotMeta& meta) {
    LOG_WARN("raft[%llu] [commit:%llu, lastindex:%llu, lastterm:%llu] starts "
             "to restore snapshot [index:%llu, term:%llu], peers: %d",
             id_, raft_log_->committed(), raft_log_->lastIndex(), raft_log_->lastTerm(),
             meta.index(), meta.term(), meta.peers_size());

    raft_log_->restore(meta.index());

    // restore all replicas
    replicas_.clear();
    learners_.clear();
    for (int i = 0; i < meta.peers_size(); ++i) {
        Peer peer;
        auto s = DecodePeer(meta.peers(i), &peer);
        if (!s.ok()) {
            return s;
        }

        if (peer.type == PeerType::kLearner) {
            // 不能从正常peer降级为learner
            if (!is_learner_ && peer.node_id == node_id_) {
                return Status(Status::kInvalidArgument, "restore meta",
                              "counld degrade from a normal peer to leadrner");
            } else {
                learners_.emplace(peer.node_id, newReplica(peer, false));
            }
        } else {
            replicas_.emplace(peer.node_id, newReplica(peer, false));
        }
    }
    return Status::OK();
}

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
