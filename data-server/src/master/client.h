_Pragma("once");

#include "resp_handler.h"

namespace sharkstore {
namespace dataserver {
namespace master {

// 请求MasterServer的各个方法
class Client {
public:
    virtual ~Client() = default;

    virtual Status Start(ResponseHandler* hanler) = 0;
    virtual Status Stop() = 0;

    // 同步接口
    virtual Status GetNodeId(const mspb::GetNodeIdRequest& req, mspb::GetNodeIdResponse& resp) = 0;
    virtual Status NodeLogin(const mspb::NodeLoginRequest& req, mspb::NodeLoginResponse& resp) = 0;
    virtual Status GetNodeAddress(const mspb::GetNodeRequest& req, mspb::GetNodeResponse& resp) = 0;
    virtual Status GetRoute(const mspb::GetRouteRequest& req, mspb::GetRouteResponse& resp) = 0;

    // 异步接口, 返回的Response会异步传递给ResponseHandler
    virtual void AsyncNodeHeartbeat(const mspb::NodeHeartbeatRequest& req) = 0;
    virtual void AsyncRangeHeartbeat(const mspb::RangeHeartbeatRequest& req) = 0;
    virtual void AsyncAskSplit(const mspb::AskSplitRequest& req) = 0;
    virtual void AsyncReportSplit(const mspb::ReportSplitRequest& req) = 0;
};

} // namespace master
} // namespace dataserver
} // namespace sharkstore