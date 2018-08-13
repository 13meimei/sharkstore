#ifndef __SOCKET_SESSION_H__
#define __SOCKET_SESSION_H__

#include <fastcommon/fast_task_queue.h>
#include <stdint.h>
#include <vector>
#include "frame/sf_socket_buff.h"

#include "socket_message.h"

namespace sharkstore {
namespace dataserver {
namespace common {

class SocketSession {
public:
    SocketSession() = default;
    virtual ~SocketSession() = default;

    SocketSession(const SocketSession &) = delete;
    SocketSession &operator=(const SocketSession &) = delete;

    virtual void Send(ProtoMessage *msg, google::protobuf::Message *resp) = 0;
};

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore

#endif  //__SOCKET_SESSION_H__
