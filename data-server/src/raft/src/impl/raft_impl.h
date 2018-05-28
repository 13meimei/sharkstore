_Pragma("once");

#include <list>
#include "raft/options.h"
#include "raft/raft.h"

#include "bulletin_board.h"
#include "raft_context.h"
#include "raft_snapshot.h"
#include "raft_types.h"
#include "ready.h"

namespace fbase {
namespace raft {
namespace impl {

class RaftFsm;

class RaftImpl : public Raft, public std::enable_shared_from_this<RaftImpl> {
public:
    RaftImpl(const RaftServerOptions& sops, const RaftOptions& ops,
             const RaftContext& context);
    ~RaftImpl();

    RaftImpl(const RaftImpl&) = delete;
    RaftImpl& operator=(const RaftImpl&) = delete;

    void Stop();
    bool IsStopped() const override { return stopped_; }

    Status TryToLeader() override;

    Status Submit(std::string& cmd) override;
    Status ChangeMemeber(const ConfChange& conf) override;

    bool IsLeader() const override { return sops_.node_id == bulletin_board_.Leader(); }

    void GetLeaderTerm(uint64_t* leader, uint64_t* term) const override {
        bulletin_board_.LeaderTerm(leader, term);
    }

    void GetPeers(std::vector<Peer>* peers) const override {
        bulletin_board_.Peers(peers);
    }

    void GetDownPeers(std::vector<DownPeer>* peers) const override {
        bulletin_board_.DownPeers(peers);
    }

    void GetPeedingPeers(std::vector<Peer>* peers) const override {
        bulletin_board_.PendingPeers(peers);
    }

    void GetStatus(RaftStatus* status) const override { bulletin_board_.Status(status); }

    void Truncate(uint64_t index) override;

    // 备份raft日志
    Status BackupLog();

    // 删除raft日志
    Status Destroy();

public:
    const RaftOptions& Options() const { return ops_; }

    void RecvMsg(MessagePtr& msg);
    void Step(MessagePtr& msg);

    void ReportSnapshotStatus(const MessagePtr& header, const SnapshotStatus& s);

private:
    void post(const std::function<void()>& f);
    bool tryPost(const std::function<void()>& f);

    void apply(bool* conf_changed);
    void smApply(const EntryPtr& e);
    void send();
    void persist();
    void apply();
    void publish();

    void truncate(uint64_t index);

private:
    const RaftServerOptions sops_;
    const RaftOptions ops_;
    const RaftContext ctx_;

    std::atomic<bool> stopped_ = {false};

    BulletinBoard bulletin_board_;

    std::unique_ptr<RaftFsm> fsm_;

    Ready ready_;
    pb::HardState prev_hard_state_;
    bool conf_changed_ = false;
    time_t last_fetch_status_ = 0;

    // TODO: manager storage_
    // TODO: manager statemachine
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
