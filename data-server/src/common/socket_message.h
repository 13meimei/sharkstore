_Pragma("once");

#include <vector>
#include <google/protobuf/message.h>

#include "proto/gen/errorpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"

#include "socket_base.h"
#include "ds_proto.h"

namespace sharkstore {
namespace dataserver {
namespace common {

struct ProtoMessage {
    int64_t session_id = 0;
    int64_t begin_time = 0;
    int64_t expire_time = 0;
    int64_t msg_id = 0;
    ds_header_t header;
    SocketBase *socket = nullptr;
    std::vector<char> body;
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
