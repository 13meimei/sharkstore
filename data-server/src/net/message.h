_Pragma("once");

#include <functional>
#include <vector>
#include <memory>
#include "protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class Session;

struct Message {
    Head head;
    std::vector<uint8_t> body;
};

using MessagePtr = std::shared_ptr<Message>;

inline MessagePtr NewMessage() { return std::make_shared<Message>(); }


struct Context {
    std::weak_ptr<Session> session;
    std::string remote_addr;
    std::string local_addr;

    // 返回true表示投递到connection，
    // false表示connection已经不存在
    bool Write(const MessagePtr& resp_msg) const;

    // 根据request的头部自动填充response的头部，发送response msg
    bool Write(const Head& req_head, std::vector<uint8_t>&& resp_body) const;
};


using Handler = std::function<void(const Context&, const MessagePtr& msg)>;

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
