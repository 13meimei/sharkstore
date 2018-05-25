_Pragma("once");

#include <list>
#include "raft/options.h"
#include "raft/status.h"

#include "raft_log.h"
#include "raft_snapshot.h"
#include "raft_types.h"
#include "replica.h"

namespace fbase {
namespace raft {
namespace impl {

class RaftFsm {
public:
    RaftFsm(const RaftServerOptions& sops, const RaftOptions& ops);
    ~RaftFsm() = default;

    RaftFsm(const RaftFsm&) = delete;
    RaftFsm& operator=(const RaftFsm&) = delete;

    void Step(MessagePtr& msg);

private:
    static int numOfPendingConf(const std::vector<EntryPtr>& ents);
    static void takeEntries(MessagePtr& msg, std::vector<EntryPtr>& ents);
    static void putEntries(MessagePtr& msg, const std::vector<EntryPtr>& ents);

    Status start();
    Status loadState(const pb::HardState& state);
    Status smApply(const EntryPtr& e);
    Status applyConfChange(const EntryPtr& e);
    Status recoverCommit();

    // 返回true表示消息处理完成
    bool stepIngoreTerm(MessagePtr& msg);
    void stepLowTerm(MessagePtr& msg);
    void stepVote(MessagePtr& msg, bool pre_vote);

    void addPeer(const pb::Peer& peer);
    void removePeer(const pb::Peer& peer);
    void updatePeer(const pb::Peer& peer);
    int quorum() const;

    // send填充msg的 id, from, term字段，然后放到待发送队列里
    void send(MessagePtr& msg);

    void reset(uint64_t term, uint64_t lasti, bool is_leader);
    void resetRandomizedElectionTimeout();
    bool pastElectionTimeout() const;

    void resetSnapshotSend();

    bool promotable() const;
    bool validate(MessagePtr& msg);

    void collect(RaftStatus* status);

private:
    void becomeLeader();
    void stepLeader(MessagePtr& msg);
    void tickHeartbeat();
    bool maybeCommit();
    void bcastAppend();
    void sendAppend(uint64_t to);
    void appendEntry(const std::vector<EntryPtr>& ents);
    void createSnapshot(uint64_t to, SnapshotRequest* snap);
    void checkDownPeers();
    void checkPendingPeers();
    void checkSnapSend();

private:
    void becomeCandidate();
    void becomePreCandidate();
    void stepCandidate(MessagePtr& msg);
    void campaign(bool pre);
    int poll(bool pre, uint64_t node_id, bool vote);

private:
    void becomeFollower(uint64_t term, uint64_t leader);
    void stepFollower(MessagePtr& msg);
    void tickElection();
    void handleAppendEntries(MessagePtr& msg);
    void handleSnapshot(MessagePtr& msg);
    Status applySnapshot(MessagePtr& msg, bool* over);
    bool checkSnapshot(const pb::SnapshotMeta& meta);
    // 从快照中恢复
    void restore(const pb::SnapshotMeta& meta);

private:
    friend class RaftImpl;

    const RaftServerOptions sops_;
    RaftOptions rops_;
    const uint64_t node_id_ = 0;
    const uint64_t id_ = 0;
    std::shared_ptr<StateMachine> sm_;

    FsmState state_ = FsmState::kFollower;
    uint64_t leader_ = 0;
    uint64_t term_ = 0;
    uint64_t vote_for_ = 0;
    bool pending_conf_ = false;
    std::shared_ptr<storage::Storage> storage_;
    std::unique_ptr<RaftLog> raft_log_;

    std::map<uint64_t, bool> votes_;
    std::map<uint64_t, std::shared_ptr<Replica>> replicas_;
    std::vector<DownPeer> down_peers_;
    std::vector<uint64_t> pending_peers_;

    unsigned election_elapsed_ = 0;
    unsigned heartbeat_elapsed_ = 0;
    unsigned rand_election_tick_ = 0;
    std::function<unsigned()> random_func_;
    std::function<void(MessagePtr&)> step_func_;
    std::function<void()> tick_func_;

    std::list<MessagePtr> pending_send_msgs_;
    std::shared_ptr<SnapshotRequest> pending_send_snap_;
    std::unique_ptr<SnapshotApplyContext> applying_snap_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
