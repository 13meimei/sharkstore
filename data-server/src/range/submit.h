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

    virtual ~SubmitContext() = default;

    SubmitContext(const SubmitContext&) = delete;
    SubmitContext& operator=(const SubmitContext&) = delete;

    int64_t SubmitTime() const { return submit_time_; }
    raft_cmdpb::CmdType Type() const { return type_; }

    // 根据之前保存的request header相关信息，填充response头部
    void FillResponseHeader(kvrpcpb::ResponseHeader* header, errorpb::Error *err);
    // 发送response
    void SendResponse(const ::google::protobuf::Message& resp);

    void CheckExecuteTime(uint64_t rangeID, int64_t thresold_usecs);

    virtual void SendError(errorpb::Error *err) = 0;

private:
    int64_t submit_time_ = 0;
    RPCRequestPtr rpc_request_;
    raft_cmdpb::CmdType type_;

    // save for set response header lately
    uint64_t cluster_id_ = 0;
    uint64_t trace_id_ = 0;
};

// 根据不同的Response类型
template <class ResponseT>
class SubmitContextT : public SubmitContext {
public:
    SubmitContextT(RPCRequestPtr rpc_request,
            raft_cmdpb::CmdType type,
            const kvrpcpb::RequestHeader& kv_header) :
        SubmitContext(std::move(rpc_request), type, kv_header)
    {}

    void SendError(errorpb::Error *err) override {
        ResponseT resp;
        SetResponseHeader(resp, err);
        SendResponse(resp);
    }
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
