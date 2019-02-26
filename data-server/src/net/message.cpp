#include "message.h"

#include "session.h"

namespace sharkstore {
namespace dataserver {
namespace net {

bool Context::Write(const MessagePtr& resp_msg) const {
    auto conn = session.lock();
    if (conn) {
        conn->Write(resp_msg);
        return true;
    } else {
        return false;
    }
}

bool Context::Write(const Head& req_head, std::vector<uint8_t>&& resp_body) const {
    auto resp_msg = NewMessage();
    resp_msg->body = std::move(resp_body);
    resp_msg->head.SetResp(req_head, resp_msg->body.size());
    return this->Write(resp_msg);
}

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
