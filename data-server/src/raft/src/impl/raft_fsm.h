_Pragma("once");

#include <list>
#include "raft/options.h"
#include "raft/status.h"

#include "raft_log.h"
#include "raft_snapshot.h"
#include "raft_types.h"
#include "replica.h"

namespace sharkstore {
namespace raft {
namespace impl {

class Ready;

class RaftFsm {
public:
    RaftFsm(const RaftServerOptions& sops, const RaftOptions& ops);
    ~RaftFsm() = default;

    RaftFsm(const RaftFsm&) = delete;
    RaftFsm& operator=(const RaftFsm&) = delete;

    bool Validate(MessagePtr& msg) const;
    void Step(MessagePtr& msg);

    // reset and return
    void GetReady(Ready* rd);

    std::tuple<uint64_t, uint64_t> GetLeaderTerm() const;

    pb::HardState GetHardState() const;
    Status Persist(bool persist_hardstate);

    std::vector<Peer> GetPeers() const;
    RaftStatus GetStatus() const;

    Status TruncateLog(uint64_t index);
    Status DestroyLog();
    Status BackupLog();

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

    bool hasReplica(uint64_t node) const;
    Replica* getReplica(uint64_t node) const;
    std::unique_ptr<Replica> newReplica(const Peer& peer, bool is_leader) const;
    void traverseReplicas(const std::function<void(uint64_t, Replica&)>& f) const;

    void addPeer(const Peer& peer);
    void removePeer(const Peer& peer);
    void promotePeer(const Peer& peer);
    int quorum() const;

    // send填充msg的 id, from, term字段，然后放到待发送队列里
    void send(MessagePtr& msg);

    void reset(uint64_t term, bool is_leader);
    void resetRandomizedElectionTimeout();
    bool pastElectionTimeout() const;

    void resetSnapshotSend();

    // 是否有资格选举为leader
    bool electable() const;
    // 过滤非法消息

private:
    void becomeLeader();
    void stepLeader(MessagePtr& msg);
    void tickHeartbeat();
    bool maybeCommit();
    void bcastAppend();
    void sendAppend(uint64_t to, Replica& pr);
    void appendEntry(const std::vector<EntryPtr>& ents);
    void createSnapshot(uint64_t to, SnapshotRequest* snap);
    void checkSnapSend();
    void checkCaughtUp();

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
    Status restore(const pb::SnapshotMeta& meta);

private:
    friend class RaftImpl;

    using TimePoint = std::chrono::time_point<std::chrono::steady_clock>;

    const RaftServerOptions sops_;
    RaftOptions rops_;
    const uint64_t node_id_ = 0;
    const uint64_t id_ = 0;
    std::shared_ptr<StateMachine> sm_;

    bool is_learner_ = false;
    FsmState state_ = FsmState::kFollower;
    uint64_t leader_ = 0;
    uint64_t term_ = 0;
    uint64_t vote_for_ = 0;
    bool pending_conf_ = false;
    std::shared_ptr<storage::Storage> storage_;
    std::unique_ptr<RaftLog> raft_log_;

    std::map<uint64_t, bool> votes_;
    std::map<uint64_t, std::unique_ptr<Replica>> replicas_;  // normal replicas
    std::map<uint64_t, std::unique_ptr<Replica>> learners_;  // learner replicas

    unsigned election_elapsed_ = 0;
    unsigned heartbeat_elapsed_ = 0;
    unsigned rand_election_tick_ = 0;
    std::function<unsigned()> random_func_;
    std::function<void(MessagePtr&)> step_func_;
    std::function<void()> tick_func_;

    std::vector<MessagePtr> sending_msgs_;
    std::shared_ptr<SnapshotRequest> sending_snap_;
    std::unique_ptr<SnapshotApplyContext> applying_snap_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
