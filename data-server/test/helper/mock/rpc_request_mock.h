_Pragma("once");

#include "common/rpc_request.h"
#include "base/status.h"
#include "proto/gen/funcpb.pb.h"

namespace sharkstore {
namespace test {
namespace mock {

class RPCFuture {
public:
    void Set(const google::protobuf::Message& resp);
    Status Get(google::protobuf::Message& resp);

private:
    bool replied_ = false;
    std::string reply_data_;
};

using RPCFuturePtr = std::shared_ptr<RPCFuture>;

class RPCRequestMock: public RPCRequest {
public:
    RPCRequestMock(const google::protobuf::Message& req,
            const RPCFuturePtr& result,
            funcpb::FunctionID func_id = funcpb::kFuncHeartbeat);

    void Reply(const google::protobuf::Message& proto_resp) override;

private:
    RPCFuturePtr future_;
};

std::pair<RPCRequestPtr, RPCFuturePtr> NewMockRPCRequest(
        const google::protobuf::Message& proto_req,
        funcpb::FunctionID func_id = funcpb::kFuncHeartbeat);

} // namespace mock
} // namespace test
} // namespace sharkstore
