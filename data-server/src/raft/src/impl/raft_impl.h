_Pragma("once");

#include <list>
#include "raft/options.h"
#include "raft/raft.h"

#include "raft.grpc.pb.h"
#include "raft_context.h"
#include "raft_snapshot.h"
#include "raft_types.h"

namespace fbase {
namespace raft {
namespace impl {

class RaftFsm;
class BulletinBoard;

class RaftImpl : public Raft, public std::enable_shared_from_this<RaftImpl> {
public:
    RaftImpl(const RaftServerOptions& sops, const RaftOptions& ops,
             const RaftContext& context);
    ~RaftImpl();

    RaftImpl(const RaftImpl&) = delete;
    RaftImpl& operator=(const RaftImpl&) = delete;

    bool IsStopped() override { return stopped_; }

    void GetLeaderTerm(uint64_t* leader, uint64_t* term) override;
    bool IsLeader() override;
    Status TryToLeader() override;

    Status Submit(std::string& cmd) override;
    Status ChangeMemeber(const ConfChange& conf) override;

    void GetPeers(std::vector<Peer>* peers) override;
    void GetDownPeers(std::vector<DownPeer>* peers) override;
    void GetPeedingPeers(std::vector<uint64_t>* peers) override;
    void GetStatus(RaftStatus* status) override;

    void Truncate(uint64_t index) override;

    void Stop();

    // 备份raft日志
    Status BackupLog();

    // 删除raft日志
    Status Destroy();

public:
    const RaftOptions& Options() const { return ops_; }

    void RecvMsg(MessagePtr& msg);
    void Step(MessagePtr& msg);

    void ReportSnapshotStatus(const MessagePtr& header,
                              const SnapshotStatus& s);

private:
    void post(const std::function<void()>& f);
    bool tryPost(const std::function<void()>& f);

    void maybeChange(uint64_t prev_leader, uint64_t prev_term);
    void persist(uint64_t prev_term, uint64_t prev_commit, uint64_t prev_vote);
    void apply(bool* conf_changed);
    void smApply(const EntryPtr& e);
    void send();

    void truncate(uint64_t index);

private:
    const RaftServerOptions sops_;
    const RaftOptions ops_;
    const RaftContext ctx_;

    std::atomic<bool> stopped_;

    RaftFsm* fsm_ = nullptr;
    BulletinBoard* bulletin_board_ = nullptr;
    time_t last_update_status_ = 0;  // 定时更新RaftStatus
};

} /* namespace impl */
} /* namespace raft */
} /* namespace fbase */
