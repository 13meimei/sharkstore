#include "raft_fsm.h"

#include <random>
#include <sstream>
#include "logger.h"
#include "raft_exception.h"
#include "replica.h"
#include "storage/storage_disk.h"
#include "storage/storage_memory.h"

namespace fbase {
namespace raft {
namespace impl {

RaftFsm::RaftFsm(const RaftServerOptions& sops, const RaftOptions& ops)
    : sops_(sops),
      rops_(ops),
      node_id_(sops.node_id),
      id_(ops.id),
      sm_(ops.statemachine) {
    auto s = start();
    if (!s.ok()) {
        throw RaftException(s);
    }
}

int RaftFsm::numOfPendingConf(const std::vector<EntryPtr>& ents) {
    int n = 0;
    for (const auto& ent : ents) {
        if (ent->type() == pb::ENTRY_CONF_CHANGE) ++n;
    }
    return n;
}

// 从Message里提取日志，使用swap，原Message里的日志内容会被清空
void RaftFsm::takeEntries(MessagePtr& msg, std::vector<EntryPtr>& ents) {
    int size = msg->entries_size();
    if (size > 0) {
        ents.reserve(size);
        for (int i = 0; i < size; ++i) {
            ents.emplace_back(std::make_shared<pb::Entry>());
            ents.back()->Swap(msg->mutable_entries(i));
        }
    }
}

void RaftFsm::putEntries(MessagePtr& msg, const std::vector<EntryPtr>& ents) {
    for (const auto& e : ents) {
        auto p = msg->add_entries();
        p->CopyFrom(*e);  // must copy
    }
}

Status RaftFsm::start() {
    // 初始化随机函数(选举超时)
    int seed = static_cast<unsigned>(
        std::chrono::system_clock::now().time_since_epoch().count() * node_id_);
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<unsigned> distribution(
        sops_.election_tick, sops_.election_tick * 2);
    random_func_ = std::bind(distribution, engine);

    // 初始化raft日志
    if (rops_.use_memory_storage) {
        storage_ = std::shared_ptr<storage::Storage>(
            new storage::MemoryStorage(id_, 40960));
        LOG_WARN("raft[%llu] use raft logger memory storage!", id_);
    } else {
        storage::DiskStorage::Options ops;
        ops.log_file_size = rops_.log_file_size;
        ops.max_log_files = rops_.max_log_files;
        ops.allow_corrupt_startup = rops_.allow_log_corrupt;
        storage_ = std::shared_ptr<storage::Storage>(
            new storage::DiskStorage(id_, rops_.storage_path, ops));
    }
    auto s = storage_->Open();
    if (!s.ok()) {
        LOG_ERROR("raft[%llu] open raft storage failed: %s", id_,
                  s.ToString().c_str());
        return Status(Status::kCorruption, "open raft logger", s.ToString());
    } else {
        raft_log_ = std::unique_ptr<RaftLog>(new RaftLog(id_, storage_));
    }

    // 加载 hardstate
    pb::HardState hs;
    s = storage_->InitialState(&hs);
    if (!s.ok()) {
        return s;
    }
    s = loadState(hs);
    if (!s.ok()) {
        return s;
    }

    // 初始化成员
    for (const auto& p : rops_.peers) {
        pb::Peer pp = toPBPeer(p);
        replicas_.emplace(pp.node_id(), std::make_shared<Replica>(pp, 0));
    }

    LOG_INFO("newRaft[%llu] commit:%llu, applied:%llu, lastindex:%llu, peers "
             "size:%d",
             id_, raft_log_->committed(), rops_.applied, raft_log_->lastIndex(),
             rops_.peers.size());

    if (rops_.applied > 0) {
        uint64_t lasti = raft_log_->lastIndex();
        if (lasti < rops_.applied) {
            raft_log_->set_committed(lasti);
            rops_.applied = lasti;
        } else if (raft_log_->committed() < rops_.applied) {
            raft_log_->set_committed(rops_.applied);
        }
        raft_log_->appliedTo(rops_.applied);
    }

    s = recoverCommit();
    if (!s.ok()) {
        return s;
    }

    if (rops_.leader == 0) {
        becomeFollower(term_, 0);
    } else if (rops_.leader != sops_.node_id) {
        becomeFollower(rops_.term, rops_.leader);
    } else {
        // 检查term，指定的leader对应的term不能小于当前term
        if (rops_.term != 0 && term_ <= rops_.term) {
            term_ = rops_.term;
            state_ = FsmState::kLeader;
            becomeLeader();
            bcastAppend();
        } else {
            becomeFollower(term_, 0);
        }
    }
    return Status::OK();
}

bool RaftFsm::stepIngoreTerm(MessagePtr& msg) {
    switch (msg->type()) {
        case pb::LOCAL_MSG_TICK:
            tick_func_();
            return true;

        case pb::LOCAL_MSG_HUP: {
            if (state_ != FsmState::kLeader && promotable()) {
                std::vector<EntryPtr> ents;
                Status s = raft_log_->slice(raft_log_->applied() + 1,
                                            raft_log_->committed() + 1,
                                            kNoLimit, &ents);
                if (!s.ok()) {
                    throw RaftException(
                        std::string("[Step] unexpected error when getting "
                                    "unapplied entries:") +
                        s.ToString());
                }

                if (numOfPendingConf(ents) != 0) {
                    LOG_INFO(
                        "raft[%llu] pending conf exist. campaign forbidden",
                        id_);
                } else {
                    campaign(sops_.enable_pre_vote);
                }
            }
            return true;
        }

        case pb::LOCAL_MSG_PROP:
            step_func_(msg);
            return true;

        case pb::HEARTBEAT_REQUEST:
            // 只接受来自leader的心跳
            if (msg->from() == leader_ && msg->from() != node_id_) {
                step_func_(msg);
            }
            return true;

        case pb::HEARTBEAT_RESPONSE:
            // 只有leader才需要处理心跳回应
            if (leader_ == node_id_ && msg->from() != node_id_) {
                step_func_(msg);
            }
            return true;

        default:
            return false;
    }
}

void RaftFsm::stepLowTerm(MessagePtr& msg) {
    if (msg->type() == pb::PRE_VOTE_REQUEST) {
        LOG_INFO("raft[%lu] [logterm: %lu, index: %lu, vote: %lu] "
                 "rejected low term %s from %lu [logterm: %lu, "
                 "index: %lu] at term %lu.",
                 id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                 MessageType_Name(msg->type()).c_str(), msg->from(),
                 msg->log_term(), msg->log_index(), term_);

        // 本节点有可能未启用PreVote，日志不新但是term很大
        // 如果丢弃该消息，就导致来源节点term升不上去，可能永远无法选举成功,
        // 我们给它回应，可以让来源节点的term升上去
        MessagePtr resp(new pb::Message);
        resp->set_type(pb::PRE_VOTE_RESPONSE);
        resp->set_to(msg->from());
        resp->set_reject(true);
        send(resp);
    } else if (sops_.enable_pre_vote &&
               msg->type() == pb::APPEND_ENTRIES_REQUEST) {
        // 收到了来自低term leader(APPEND只会由leader发出)的消息，原因可能是
        // 1) 来源leader被分区出去
        // 2) 本节点选举增加了term，因为启用了PreVote，不会随意提升term
        // 两种情况让来源leader退位是安全的
        // 如果它是一个被网络延迟的消息，我们的回复里log_term和commit都是0
        // 也不会干扰正常leader的复制，让leader错误以为前面发起的某次复制是成功的

        LOG_INFO("raft[%llu] ignore a [%s] message from low term leader [%llu "
                 "term: %llu] at term %llu",
                 id_, MessageType_Name(msg->type()).c_str(), msg->from(),
                 msg->term(), term_);

        MessagePtr resp(new pb::Message);
        resp->set_type(pb::APPEND_ENTRIES_RESPONSE);
        resp->set_to(msg->from());
        send(resp);
    } else {
        LOG_INFO("raft[%llu] ignore a [%s] message with lower term "
                 "from [%llu term: %llu] at term %llu",
                 id_, MessageType_Name(msg->type()).c_str(), msg->from(),
                 msg->term(), term_);
    }
}

void RaftFsm::stepVote(MessagePtr& msg, bool pre_vote) {
    // 如果非PreVote，此时msg term已经和当前term一致

    MessagePtr resp(new pb::Message);
    resp->set_type(pre_vote ? pb::PRE_VOTE_RESPONSE : pb::VOTE_RESPONSE);
    resp->set_to(msg->from());

    // 可以投票条件：
    // 1)  当前term已经给它投过票
    // 2)  当前term没给其他人投过票，且当前term没有leader
    // 3)  如果是prevote则需要msg的term(加过1的)大于我们的term
    bool can_vote =
        (vote_for_ == msg->from()) || (vote_for_ == 0 && leader_ == 0) ||
        (msg->type() == pb::PRE_VOTE_REQUEST && msg->term() > term_);

    // 比较日志新旧
    if (can_vote &&
        raft_log_->isUpdateToDate(msg->log_index(), msg->log_term())) {
        LOG_INFO("raft[%llu] [logterm:%llu, index:%llu, vote:%llu] %s for"
                 "%llu[logterm:%llu, index:%llu] at term %llu",
                 id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                 (pre_vote ? "vote(pre)" : "vote"), msg->from(),
                 msg->log_term(), msg->log_index(), term_);

        resp->set_term(msg->term());
        resp->set_reject(false);
        send(resp);

        if (msg->type() == pb::VOTE_REQUEST) {
            vote_for_ = msg->from();
            election_elapsed_ = 0;
        }
    } else {
        LOG_INFO("raft[%llu] [logterm:%llu, index:%llu, vote:%llu] %s for "
                 "%llu[logterm:%llu, index:%llu] at term %llu",
                 id_, raft_log_->lastTerm(), raft_log_->lastIndex(), vote_for_,
                 (pre_vote ? "reject vote(pre)" : "reject vote"), msg->from(),
                 msg->log_term(), msg->log_index(), term_);

        resp->set_term(term_);
        resp->set_reject(true);
        send(resp);
    }
}

void RaftFsm::Step(MessagePtr& msg) {
    // 处理不需要关心term的消息类型
    if (stepIngoreTerm(msg)) {
        return;
    }

    // 处理低term msg
    if (msg->term() < term_) {
        stepLowTerm(msg);
        return;
    }

    // 高term，先变为follower再处理msg
    if (msg->term() > term_) {
        if (msg->type() == pb::PRE_VOTE_REQUEST ||
            (msg->type() == pb::PRE_VOTE_RESPONSE && !msg->reject())) {
            // 1) prevote请求的term是来源自身的term+1，不是其真正的term
            // 2) 如果是prevote回应是成功，回应的term是请求时的term（即+1后的）
            //    我们是收到大多数prevote成功后才会增加term，不应该此时加
        } else {
            LOG_INFO("raft[%llu] received a [%s] message with higher term "
                     "from [%llu term: %llu] at term %llu, become follower",
                     id_, MessageType_Name(msg->type()).c_str(), msg->from(),
                     msg->term(), term_);

            if (msg->type() == pb::APPEND_ENTRIES_REQUEST ||
                msg->type() == pb::SNAPSHOT_REQUEST) {
                becomeFollower(msg->term(), msg->from());
            } else {
                becomeFollower(msg->term(), 0);
            }
        }
    }

    if (msg->type() == pb::PRE_VOTE_REQUEST) {
        stepVote(msg, true);
    } else if (msg->type() == pb::VOTE_REQUEST) {
        stepVote(msg, false);
    } else {
        step_func_(msg);
    }
}

Status RaftFsm::loadState(const pb::HardState& state) {
    // 全空
    if (state.commit() == 0 && state.term() == 0 && state.vote() == 0) {
        return Status::OK();
    }

    if (state.commit() < raft_log_->committed() ||
        state.commit() > raft_log_->lastIndex()) {
        std::stringstream ss;
        ss << "loadState: state.commit " << state.commit()
           << " is out of range [" << raft_log_->committed() << ", "
           << raft_log_->lastIndex() << "]";
        return Status(Status::kCorruption, ss.str(), "");
    }

    term_ = state.term();
    vote_for_ = state.vote();
    raft_log_->commitTo(state.commit());

    return Status::OK();
}

static Status getConfChange(const EntryPtr& entry, pb::ConfChange* cc) {
    assert(entry->type() == pb::ENTRY_CONF_CHANGE);
    if (!cc->ParseFromString(entry->data())) {
        return Status(Status::kCorruption, "parse confchange data failed", "");
    }
    return Status::OK();
}

Status RaftFsm::smApply(const EntryPtr& entry) {
    Status s;
    switch (entry->type()) {
        case pb::ENTRY_NORMAL:
            if (!entry->data().empty()) {
                return sm_->Apply(entry->data(), entry->index());
            }
            break;

        case pb::ENTRY_CONF_CHANGE: {
            pb::ConfChange pbcc;
            s = getConfChange(entry, &pbcc);
            if (!s.ok()) {
                return s;
            }
            ConfChange cc;
            fromPBCC(pbcc, &cc);
            s = sm_->ApplyMemberChange(cc, entry->index());
            if (!s.ok()) {
                return s;
            }
            break;
        }
        default:
            return Status(Status::kInvalidArgument,
                          "apply unknown raft entry type",
                          std::to_string(static_cast<int>(entry->type())));
    }
    return s;
}

Status RaftFsm::applyConfChange(const EntryPtr& e) {
    assert(e->type() == pb::ENTRY_CONF_CHANGE);

    LOG_INFO("raft[%llu] apply confchange at index %lu, term %lu", id_,
             e->index(), term_);

    pb::ConfChange cc;
    auto s = getConfChange(e, &cc);
    if (!s.ok()) return s;

    if (cc.peer().node_id() == 0) {  // invalid peer id
        pending_conf_ = false;
        return Status(Status::kInvalidArgument,
                      "apply confchange invalid node id", "0");
    }
    switch (cc.type()) {
        case pb::CONF_ADD_NODE:
            addPeer(cc.peer());
            break;
        case pb::CONF_REMOVE_NODE:
            removePeer(cc.peer());
            break;
        case pb::CONF_UPDATE_NODE:
            updatePeer(cc.peer());
            break;
        default:
            return Status(Status::kInvalidArgument,
                          "apply confchange invalid cc type",
                          pb::ConfChangeType_Name(cc.type()));
    }
    return Status::OK();
}

Status RaftFsm::recoverCommit() {
    Status s;
    while (raft_log_->applied() < raft_log_->committed()) {
        std::vector<EntryPtr> committed_entries;
        raft_log_->nextEntries(64 * 1024 * 1024, &committed_entries);
        for (const auto& entry : committed_entries) {
            raft_log_->appliedTo(entry->index());
            if (entry->type() == pb::ENTRY_CONF_CHANGE) {
                s = applyConfChange(entry);
                if (!s.ok()) {
                    return s;
                }
            }
            s = smApply(entry);
            if (!s.ok()) {
                return s;
            }
        }
    }
    return s;
}

void RaftFsm::addPeer(const pb::Peer& peer) {
    LOG_INFO("raft[%llu] add peer [node:%lu, peer:%lu]", id_, peer.node_id(),
             peer.peer_id());

    pending_conf_ = false;

    if (replicas_.find(peer.node_id()) == replicas_.end()) {
        int max_inflight =
            state_ == FsmState::kLeader ? sops_.max_inflight_msgs : 0;

        replicas_[peer.node_id()] =
            std::make_shared<Replica>(peer, max_inflight);

        if (state_ == FsmState::kLeader) {
            replicas_[peer.node_id()]->set_next(raft_log_->lastIndex() + 1);
        }
    } else {
        LOG_INFO("raft[%llu] add exsist peer %llu", id_, peer.node_id());
    }
}

void RaftFsm::removePeer(const pb::Peer& peer) {
    LOG_INFO("raft[%llu] remove peer [node:%llu, peer:%llu]", id_,
             peer.node_id(), peer.peer_id());

    pending_conf_ = false;

    auto it = replicas_.find(peer.node_id());
    if (it == replicas_.end()) {  // 没有该node id
        return;
    } else if (it->second->peer().peer_id() != peer.peer_id()) {
        // peer id不一致，不删除，防止日志重放时误删除
        LOG_INFO("raft[%llu] ignore remove peer [node:%llu, peer:%llu], "
                 "current[peer:%llu]",
                 id_, peer.node_id(), peer.peer_id(),
                 it->second->peer().peer_id());
        return;
    } else {
        replicas_.erase(it);
        if (peer.node_id() == node_id_) {
            becomeFollower(term_, 0);
        } else if (state_ == FsmState::kLeader && !replicas_.empty()) {
            if (maybeCommit()) {
                bcastAppend();
            }
        }
    }
}

void RaftFsm::updatePeer(const pb::Peer& peer) {
    LOG_INFO("raft[%llu] update peer %llu", id_, peer.node_id());

    pending_conf_ = false;
    auto it = replicas_.find(peer.node_id());
    if (it != replicas_.end()) {
        it->second->set_peer(peer);
    }
}

int RaftFsm::quorum() const { return replicas_.size() / 2 + 1; }

void RaftFsm::send(MessagePtr& msg) {
    msg->set_id(id_);
    msg->set_from(node_id_);

    // LOCAL_MSG_PROP消息term为0
    // 其他类型如果已指定term则使用原msg的term
    if (msg->type() != pb::LOCAL_MSG_PROP && msg->term() == 0) {
        msg->set_term(term_);
    }

    pending_send_msgs_.push_back(msg);
}

void RaftFsm::reset(uint64_t term, uint64_t lasti, bool is_leader) {
    if (term_ != term) {
        term_ = term;
        vote_for_ = 0;
    }

    leader_ = 0;
    election_elapsed_ = 0;
    heartbeat_elapsed_ = 0;
    votes_.clear();
    pending_conf_ = false;
    resetSnapshotSend();

    if (is_leader) {
        rand_election_tick_ = sops_.election_tick - 1;
        for (auto& r : replicas_) {
            pb::Peer peer = r.second->peer();
            r.second.reset(new Replica(peer, sops_.max_inflight_msgs));
            r.second->set_next(lasti + 1);
            if (peer.node_id() == node_id_) {
                r.second->set_match(lasti);
                r.second->set_committed(raft_log_->committed());
            }
        }
    } else {
        resetRandomizedElectionTimeout();
        for (auto& r : replicas_) {
            pb::Peer peer = r.second->peer();
            r.second.reset(new Replica(peer, 0));
        }
    }
}

void RaftFsm::resetRandomizedElectionTimeout() {
    rand_election_tick_ = random_func_();
    LOG_DEBUG("raft[%llu] election tick reset to %d", id_, rand_election_tick_);
}

bool RaftFsm::pastElectionTimeout() const {
    return election_elapsed_ >= rand_election_tick_;
}

void RaftFsm::resetSnapshotSend() {
    if (pending_send_snap_) {
        pending_send_snap_->Cancel();
        pending_send_snap_.reset();
    }
}

bool RaftFsm::promotable() const {
    // 是否还在集群中
    return replicas_.find(node_id_) != replicas_.cend();
}

bool RaftFsm::validate(MessagePtr& msg) {
    // 过滤未知来源的response
    // 不过滤request是因为副本有可能落后，集群成员不是最新的
    return IsLocalMsg(msg) ||
           (replicas_.find(msg->from()) != replicas_.cend()) ||
           !IsResponseMsg(msg);
}

void RaftFsm::collect(RaftStatus* status) {
    status->node_id = sops_.node_id;
    status->leader = leader_;
    status->term = term_;
    status->index = raft_log_->lastIndex();
    status->commit = raft_log_->committed();
    status->applied = raft_log_->applied();
    status->state = FsmStateName(state_);
    if (leader_ == sops_.node_id) {  // is leader
        for (const auto& p : replicas_) {
            if (p.first == node_id_) continue;
            ReplicaStatus rs;
            rs.peer_id = p.second->peer().peer_id();
            rs.match = p.second->match();
            rs.commit = p.second->committed();
            rs.next = p.second->next();
            rs.inactive = p.second->inactive_seconds();
            rs.state = ReplicateStateName(p.second->state());
            status->replicas.emplace(p.first, rs);
        }
    }
}

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
