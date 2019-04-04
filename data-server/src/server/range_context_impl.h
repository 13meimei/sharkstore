_Pragma("once");

#include "range/context.h"
#include "context_server.h"
#include "run_status.h"
#include "range_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class RangeContextImpl : public range::RangeContext {
public:
    explicit RangeContextImpl(ContextServer *s);

    uint64_t GetNodeID() const override { return server_->node_id; }

    range::SplitPolicy* GetSplitPolicy() override { return split_policy_.get(); }

    storage::DbInterface *DBInstance(const uint64_t flag = 0) override {
        return flag==0? server_->db: server_->pdb;
    }
    master::Worker* MasterClient() override  { return server_->master_worker; }
    raft::RaftServer* RaftServer() override { return server_->raft_server; }
    storage::MetaStore* MetaStore() override { return server_->meta_store; }
    range::RangeStats* Statistics(const uint64_t flag = 0) override {
        return flag==0?server_->run_status:server_->persist_run_status;
    }
	watch::WatchServer* WatchServer() override { return server_->range_server->watch_server_; }
    server::PersistServer* PersistServer() override { return server_->persist_server; }

    uint64_t GetFSUsagePercent(const uint64_t seq) const override;

    void ScheduleHeartbeat(uint64_t range_id, bool delay) override;
    void ScheduleCheckSize(uint64_t range_id) override;

    // range manage
    std::shared_ptr<range::Range> FindRange(uint64_t range_id) override;

    // split
    Status SplitRange(uint64_t range_id, const raft_cmdpb::SplitRequest &req,
            uint64_t raft_index) override;

private:
    ContextServer* server_ = nullptr;
    std::unique_ptr<range::SplitPolicy> split_policy_;
};


}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
