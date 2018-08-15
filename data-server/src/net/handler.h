_Pragma("once");

#include <functional>
#include "protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class Session;

struct Context {
    std::weak_ptr<Session> session;
    std::string remote_addr;
    std::string local_addr;
};

struct Message {
    Head head;
    std::vector<uint8_t> body;

    Message() = default;
    Message(const Message&) = default;
    Message& operator=(const Message&) = default;

    Message(Message&& msg) {
        *this = std::move(msg);
    }

    Message& operator=(Message&& msg) {
        if (this != &msg) {
            head = msg.head;
            body = std::move(msg.body);
        }
        return *this;
    }
};

using Handler = std::function<void(const Context&, Message&& msg)>;

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
