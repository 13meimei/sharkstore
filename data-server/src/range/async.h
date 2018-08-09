_Pragma("once");

#include "context.h"

namespace sharkstore {
namespace dataserver {
namespace range {

class AsyncContext {
public:
    AsyncContext(raft_cmdpb::CmdType type, RangeContext* rc,
                 common::ProtoMessage *msg, kvrpcpb::RequestHeader *req);

    ~AsyncContext() {
        if (proto_message != nullptr) delete proto_message;
        if (request_header != nullptr) delete request_header;

        if (submit_time > 0) {
            range_context->Statistics()->PushTime(monitor::PrintTag::Raft,
                                                 get_micro_second() - submit_time);
        }
    }

    common::ProtoMessage *release_proto_message() {
        auto msg = proto_message;
        proto_message = nullptr;
        return msg;
    }

    kvrpcpb::RequestHeader *release_request_header() {
        auto rh = request_header;
        request_header = nullptr;
        return rh;
    }

private:
    raft_cmdpb::CmdType cmd_type_;
    RangeContext *range_context_ = nullptr;
    common::ProtoMessage *proto_message_ = nullptr;
    kvrpcpb::RequestHeader *request_header_ = nullptr;
    uint64_t submit_time_ = 0;
};

class AsyncContextQueue {
public:

    AsyncContext *Add(raft_cmdpb::CmdType type,
            common::ProtoMessage *msg,
            kvrpcpb::RequestHeader *req);

    AsyncContext *Release(uint64_t seq_id);
    void Delete(uint64_t seq_id);

private:
    using ExpirePair = std::pair<time_t, uint64_t>;
    using ExpireQueue = std::priority_queue<ExpirePair, std::vector<ExpirePair>, std::greater<ExpirePair>>;
    using ContextMap = std::unordered_map<uint64_t, AsyncContext*>;

    std::atomic<uint64_t> seq_ = {1};
    ContextMap ctx_map_;
    ExpireQueue expire_que_;
    std::mutex mu_;
};


}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
