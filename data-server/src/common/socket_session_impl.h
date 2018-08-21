#ifndef __SOCKET_SESSION_IMPL_H__
#define __SOCKET_SESSION_IMPL_H__

#include "socket_session.h"

namespace sharkstore {
namespace dataserver {
namespace common {


class SocketSessionImpl : public SocketSession {
public:
    SocketSessionImpl() = default;
    ~SocketSessionImpl() = default;

    SocketSessionImpl(const SocketSessionImpl&) = delete;
    SocketSessionImpl& operator=(const SocketSessionImpl&) = delete;

    void Send(ProtoMessage *msg, google::protobuf::Message* resp) override;
    void WatchSend(ProtoMessage *msg, google::protobuf::Message *resp);

    bool GetMessage(const char *data, size_t size,
            google::protobuf::Message* req);

    void SetResponseHeader(const kvrpcpb::RequestHeader &req,
            kvrpcpb::ResponseHeader *resp,  errorpb::Error *err = nullptr);

};

} //namespace common
} //namespace dataserver
} //namespace sharkstore

#endif //__SOCKET_SESSION_IMPL_H__
