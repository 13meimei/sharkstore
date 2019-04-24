_Pragma("once");

#include "proto/gen/mspb.grpc.pb.h"

namespace sharkstore {
namespace dataserver {
namespace master {

class ResponseHandler {
public:
    virtual ~ResponseHandler() = default;

    virtual void OnNodeHeartbeatResp(const mspb::NodeHeartbeatResponse&) = 0;
    virtual void OnRangeHeartbeatResp(const mspb::RangeHeartbeatResponse&) = 0;
    virtual void OnAskSplitResp(const mspb::AskSplitResponse&) = 0;
};

}  // namespace master
}  // namespace dataserver
}  // namespace sharkstore
