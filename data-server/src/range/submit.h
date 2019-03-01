_Pragma("once");

#include <queue>
#include <vector>
#include <unordered_map>
#include <mutex>

#include "proto/gen/raft_cmdpb.pb.h"
#include "common/rpc_request.h"

namespace sharkstore {
namespace dataserver {
namespace range {

// 提交raft命令之前创建一个SubmitContext保存RPC等上下文到队列中
// 等复制并Apply完成后，再从队列里拿出来给客户端回应
class SubmitContext {
public:
    SubmitContext(RPCRequestPtr rpc_request,
            raft_cmdpb::CmdType type,
            const kvrpcpb::RequestHeader& kv_header);

    SubmitContext(const SubmitContext&) = delete;
    SubmitContext& operator=(const SubmitContext&) = delete;

    int64_t SubmitTime() const { return submit_time_; }
    raft_cmdpb::CmdType Type() const { return type_; }

    template <class ResponseT>
    void Reply(ResponseT& resp, errorpb::Error *err = nullptr) {
        // 设置response头部
        SetResponseHeader(resp.mutable_header(), cluster_id_, trace_id_, err);
        rpc_request_->Reply(resp);
    }

    void SendError(errorpb::Error *err);
    void SendTimeout();

    void CheckExecuteTime(uint64_t rangeID, int64_t thresold_usecs);

private:
    int64_t submit_time_ = 0;
    RPCRequestPtr rpc_request_;
    raft_cmdpb::CmdType type_;

    // save for set response header lately
    uint64_t cluster_id_ = 0;
    uint64_t trace_id_ = 0;
};

class SubmitQueue {
public:
    SubmitQueue() = default;
    ~SubmitQueue() = default;

    SubmitQueue(const SubmitQueue&) = delete;
    SubmitQueue& operator=(const SubmitQueue&) = delete;

    // 只获取一个递增的ID, for split command
    uint64_t GetSeq();

    uint64_t Add(RPCRequestPtr rpc_request, raft_cmdpb::CmdType type,
            const kvrpcpb::RequestHeader& kv_req_header);

    std::unique_ptr<SubmitContext> Remove(uint64_t seq_id);

    std::vector<uint64_t> GetExpired(size_t max_count = 10000);

    size_t Size() const;

private:
    using ExpirePair = std::pair<time_t, uint64_t>;
    using ExpireQueue = std::priority_queue<ExpirePair, std::vector<ExpirePair>, std::greater<ExpirePair>>;
    using SubmitContextPtr = std::unique_ptr<SubmitContext>;
    using ContextMap = std::unordered_map<uint64_t, SubmitContextPtr>;

    uint64_t seq_ = 0;
    ContextMap ctx_map_;
    ExpireQueue expire_que_;
    mutable std::mutex mu_;
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
