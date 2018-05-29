_Pragma("once");

#include <atomic>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include "base/shared_mutex.h"
#include "raft/server.h"
#include "transport/transport.h"

namespace sharkstore {
namespace raft {
namespace impl {

class RaftImpl;
class WorkThread;
class SnapshotSender;

class RaftServerImpl : public RaftServer {
public:
    explicit RaftServerImpl(const RaftServerOptions& ops);
    ~RaftServerImpl();

    RaftServerImpl(const RaftServerImpl&) = delete;
    RaftServerImpl& operator=(const RaftServerImpl&) = delete;

    Status Start() override;
    Status Stop() override;

    const RaftServerOptions& Options() const override { return ops_; }

    Status CreateRaft(const RaftOptions&, std::shared_ptr<Raft>* raft) override;
    Status RemoveRaft(uint64_t id, bool backup) override;
    std::shared_ptr<Raft> FindRaft(uint64_t id) const override;

    void GetStatus(ServerStatus* status) override;

private:
    typedef std::unordered_map<uint64_t, std::shared_ptr<RaftImpl>> RaftMap;

    std::shared_ptr<RaftImpl> findRaft(uint64_t id) const;

    void sendHeartbeat(const RaftMap& rafts);
    void onMessage(MessagePtr& msg);
    void onHeartbeatReq(MessagePtr& msg);
    void onHeartbeatResp(MessagePtr& msg);

    void stepTick(const RaftMap& rafts);
    void printMetrics();
    void tickRoutine();

private:
    const RaftServerOptions ops_;
    std::atomic<bool> running_;

    transport::Transport* transport_ = nullptr;
    SnapshotSender* snapshot_sender_ = nullptr;

    RaftMap rafts_;
    std::unordered_set<uint64_t> creatings_;  // 正在被创建的
    uint64_t create_count_ = 0;
    mutable sharkstore::shared_mutex mu_;

    std::vector<WorkThread*> consensus_threads_;
    std::vector<WorkThread*> apply_threads_;

    std::unique_ptr<std::thread> tick_thr_;
    MessagePtr tick_msg_;
};

} /* namespace impl */
} /* namespace raft */
} /* namespace sharkstore */
