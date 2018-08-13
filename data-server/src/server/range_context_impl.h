_Pragma("once");

#include "range/context.h"
#include "context_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class RangeContextImpl : public range::RangeContext {
public:
    explicit RangeContextImpl(ContextServer *s) : server_(s) {}

    // split parameters
    uint64_t SplitSize() const override;
    uint64_t MaxSize() const override;

    uint64_t GetNodeID() const override { return server_->node_id; }

    rocksdb::DB *DBInstance() const() override { return server_->rocks_db; }
    master::Worker* MasterClient() override  { return server_->master_worker; }
    raft::RaftServer* RaftServer() override { return server_->raft_server; }
    storage::MetaStore* MetaStore() override { return server_->meta_store; }
    common::SocketSession* SocketSession() override { return server_->socket_session; }
    Statistics* Statistics() override { return server_->run_status; }

    void ScheduleHeartbeat(uint64_t range_id, bool delay) override;
    void ScheduleCheckSize(uint64_t range_id) override;

    // range manage
    std::shared_ptr<Range> FindRange(uint64_t range_id) override;

    // split
    Status SplitRange(uint64_t range_id, const raft_cmdpb::SplitRequest &req,
                  uint64_t raft_index, std::shared_ptr<Range> *new_range) override;

private:
    ContextServer* server_ = nullptr;
};


}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
