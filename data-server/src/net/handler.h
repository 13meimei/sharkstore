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

using Handler = std::function<void(const Context&, const Head&, std::vector<uint8_t>&&)>;

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
