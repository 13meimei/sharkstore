#ifndef __SOCKET_SESSION_H__
#define __SOCKET_SESSION_H__

#include <fastcommon/fast_task_queue.h>
#include <stdint.h>
#include <vector>
#include "frame/sf_socket_buff.h"
#include "proto/gen/errorpb.pb.h"
#include "proto/gen/kvrpcpb.pb.h"

#include "ds_proto.h"
#include "socket_base.h"

namespace sharkstore {
namespace dataserver {
namespace common {

typedef struct ProtoMessage_{
    int64_t session_id;
    int64_t begin_time;
    int64_t expire_time;
    int64_t msg_id;
    ds_header_t header;
    SocketBase *socket;
    std::vector<char> body;

    ProtoMessage_(){};
    ProtoMessage_( const struct ProtoMessage_ &other ) {
        this->session_id = other.session_id;
        this->begin_time = other.begin_time;
        this->expire_time = other.expire_time;
        this->msg_id = other.msg_id;
        this->header = other.header;
        this->socket = other.socket;
        this->body.assign(other.body.begin(), other.body.end());
    }
} ProtoMessage;

class SocketSession {
public:
    SocketSession() = default;
    virtual ~SocketSession() = default;

    SocketSession(const SocketSession &) = delete;
    SocketSession &operator=(const SocketSession &) = delete;

    virtual ProtoMessage *GetProtoMessage(const void *data) = 0;

    virtual void Send(ProtoMessage *msg, google::protobuf::Message *resp) = 0;

    virtual bool GetMessage(const char *data, size_t size,
                            google::protobuf::Message *req) = 0;

    virtual void SetResponseHeader(const kvrpcpb::RequestHeader &req,
                                   kvrpcpb::ResponseHeader *resp,
                                   errorpb::Error *err = nullptr) = 0;
};

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore

#endif  //__SOCKET_SESSION_H__
