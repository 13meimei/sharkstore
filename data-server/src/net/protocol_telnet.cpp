#include "protocol_telnet.h"

namespace fbase {
namespace dataserver {
namespace net {

bool ProtocolTelnet::Match(const std::array<uint8_t, 4>& preface) {
    // always true
    return true;
}

void ProtocolTelnet::OnDataArrived(const uint8_t* buf, size_t len) {
    // TODO:
}

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
