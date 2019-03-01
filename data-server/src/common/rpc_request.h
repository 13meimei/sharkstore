_Pragma("once");

#include <vector>
#include <google/protobuf/message.h>

#include "proto/gen/errorpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"

#include "frame/sf_util.h"
#include "net/message.h"

namespace sharkstore {
namespace dataserver {

// RPC请求默认处理超时
static const uint32_t kDefaultRPCRequestTimeoutMS = 10000;

// 一次RPC网络请求
struct RPCRequest {
    net::Context ctx;
    net::MessagePtr msg;
    int64_t expire_time = 0; // 过期绝对时间，单位毫秒
    int64_t begin_time = 0;  // 请求开始时间，单位微秒

    RPCRequest(const net::Context& req_ctx, const net::MessagePtr& req_msg);

    // Parse to request proto msg
    bool ParseTo(google::protobuf::Message& proto_req, bool zero_copy = true);

    // Send response
    void Reply(const google::protobuf::Message& proto_resp);
};

using RPCRequestPtr = std::unique_ptr<RPCRequest>;


// 设置ResponseHeader字段
void SetResponseHeader(kvrpcpb::ResponseHeader* resp,
                       const kvrpcpb::RequestHeader &req,
                       errorpb::Error *err = nullptr);

void SetResponseHeader(kvrpcpb::ResponseHeader* resp,
                       uint64_t cluster_id, uint64_t trace_id,
                       errorpb::Error *err = nullptr);

}  // namespace dataserver
}  // namespace sharkstore
