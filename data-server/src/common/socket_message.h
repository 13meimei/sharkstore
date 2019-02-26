_Pragma("once");

#include <vector>
#include <google/protobuf/message.h>

#include "proto/gen/errorpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"

#include "frame/sf_util.h"
#include "net/message.h"

namespace sharkstore {
namespace dataserver {
namespace common {

struct ProtoMessage {
    net::Context ctx;
    net::MessagePtr msg;
};

// 从报文数据中解析生成ProtoMessage
ProtoMessage *GetProtoMessage(const void *data);

// 反序列化
bool GetMessage(const char *data, size_t size,
        google::protobuf::Message *req, bool zero_copy = true);

// 设置ResponseHeader字段
void SetResponseHeader(const kvrpcpb::RequestHeader &req,
        kvrpcpb::ResponseHeader *resp,
        errorpb::Error *err = nullptr);

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
