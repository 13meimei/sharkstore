_Pragma("once");

#include <mutex>
#include <condition_variable>
#include <chrono>

#include "common/rpc_request.h"
#include "base/status.h"
#include "proto/gen/funcpb.pb.h"

namespace sharkstore {
namespace test {
namespace mock {

class RPCFutureWait {
public:
    void Set(const google::protobuf::Message& resp);
    Status Get(google::protobuf::Message& resp);

private:
    bool replied_ = false;
    std::string reply_data_;
    
    std::mutex mu_;
    std::condition_variable cv_; 
};

using RPCFutureWaitPtr = std::shared_ptr<RPCFutureWait>;

class RPCRequestWaitMock: public RPCRequest {
public:
    RPCRequestWaitMock(const google::protobuf::Message& req,
            const RPCFutureWaitPtr& result,
            funcpb::FunctionID func_id = funcpb::kFuncHeartbeat);

    void Reply(const google::protobuf::Message& proto_resp) override;

private:
    RPCFutureWaitPtr future_;
};

// 返回一个RPCRequestWait用于向下传递处理请求
// 返回一个RPCFutureWait用于获取处理结果
std::pair<RPCRequestPtr, RPCFutureWaitPtr> NewMockRPCRequestWait(
        const google::protobuf::Message& proto_req,
        funcpb::FunctionID func_id = funcpb::kFuncHeartbeat);

} // namespace mock
} // namespace test
} // namespace sharkstore
