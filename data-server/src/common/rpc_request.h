_Pragma("once");

#include <vector>
#include <google/protobuf/message.h>

#include "proto/gen/errorpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"

#include "frame/sf_util.h"
#include "net/message.h"

namespace sharkstore {
namespace dataserver {

// 一次RPC网络请求
struct RPCRequest {
    net::Context ctx;
    net::MessagePtr msg;
    int64_t expire_time = 0; // TODO:
    int64_t begin_time = 0;

    RPCRequest(const net::Context& req_ctx, const net::MessagePtr& req_msg);

    // Parse to request proto msg
    bool ParseTo(google::protobuf::Message& proto_req, bool zero_copy = true);

    // Send response
    void Reply(const google::protobuf::Message& proto_resp);
};

using RPCRequestPtr = std::unique_ptr<RPCRequest>;


// 反序列化
bool GetMessage(const char *data, size_t size,
        google::protobuf::Message *req, bool zero_copy = true);

// 设置ResponseHeader字段
void SetResponseHeader(const kvrpcpb::RequestHeader &req,
        kvrpcpb::ResponseHeader *resp,
        errorpb::Error *err = nullptr);

}  // namespace dataserver
}  // namespace sharkstore
