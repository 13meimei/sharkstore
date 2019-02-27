_Pragma("once");

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/rpc_request.h"
#include "proto/gen/mspb.pb.h"
#include "proto/gen/raft_cmdpb.pb.h"

#include "base/shared_mutex.h"
#include "base/status.h"
#include "master/task_handler.h"
#include "range/range.h"
#include "storage/meta_store.h"

#include "server/context_server.h"
#include "watch/watch_server.h"

namespace sharkstore {
namespace dataserver {
namespace server {

class RangeServer final : public master::TaskHandler {
public:
    RangeServer() = default;
    ~RangeServer() = default;

    RangeServer(const RangeServer &) = delete;
    RangeServer &operator=(const RangeServer &) = delete;
    RangeServer &operator=(const RangeServer &) volatile = delete;

    int Init(ContextServer *context);
    int Start();
    void Stop();
    void Clear();

    void DealTask(std::unique_ptr<RPCRequest> request);
    void StatisPush(uint64_t range_id);

    storage::MetaStore *meta_store() { return meta_store_; }

    size_t GetRangesSize() const;
    std::shared_ptr<range::Range> Find(uint64_t range_id);

    void OnNodeHeartbeatResp(const mspb::NodeHeartbeatResponse &) override;
    void OnRangeHeartbeatResp(const mspb::RangeHeartbeatResponse &) override;
    void OnAskSplitResp(const mspb::AskSplitResponse &) override;
    void CollectNodeHeartbeat(mspb::NodeHeartbeatRequest *req) override;

private:
    void buildDBOptions(rocksdb::Options& ops);
    int OpenDB();
    void CloseDB();

    Status recover(const metapb::Range& meta);
    int recover(const std::vector<metapb::Range> &metas);

    void TimeOut(const kvrpcpb::RequestHeader &req,
                 kvrpcpb::ResponseHeader *resp);
    void RangeNotFound(const kvrpcpb::RequestHeader &req,
                       kvrpcpb::ResponseHeader *resp);

    template <class RequestT, class ResponseT>
    std::shared_ptr<range::Range> RangeServer::DecodeAndFind(
            const std::unique_ptr<RPCRequest>& req, RequestT& proto_req, const char* func_name);

public:
    Status SplitRange(uint64_t old_range_id, const raft_cmdpb::SplitRequest &req,
            uint64_t raft_index);

    void LeaderQueuePush(uint64_t leader, time_t expire);

private:  // admin
    void CreateRange(RPCRequest& req);
    void DeleteRange(RPCRequest& req);
    void OfflineRange(RPCRequest& req);
    void ReplaceRange(RPCRequest& req);
    void TransferLeader(RPCRequest& req);
    void GetPeerInfo(RPCRequest& req);
    void SetLogLevel(RPCRequest& req);

    Status CreateRange(const metapb::Range &range, uint64_t leader = 0, uint64_t log_start_index = 0);
    Status DeleteRange(uint64_t range_id, uint64_t peer_id = 0);
    int CloseRange(uint64_t range_id);
    int OfflineRange(uint64_t range_id);
    void Heartbeat();

private:
    mutable shared_mutex rw_lock_;
    std::unordered_map<int64_t, std::shared_ptr<range::Range>> ranges_;

    std::mutex statis_mutex_;
    std::condition_variable statis_cond_;
    std::queue<uint64_t> statis_queue_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cond_;

    typedef std::pair<time_t, uint64_t> tr;
    std::priority_queue<tr, std::vector<tr>, std::greater<tr>>
        range_heartbeat_queue_;

    std::vector<std::thread> worker_;
    std::thread range_heartbeat_;

    rocksdb::DB *db_ = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;
    storage::MetaStore *meta_store_ = nullptr;

    ContextServer *context_ = nullptr;
    std::unique_ptr<range::RangeContext> range_context_;

public:
    watch::WatchServer* watch_server_;
};

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore

#endif  //__RANGE_MANAGER_H__
