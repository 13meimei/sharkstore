#include "protocol_rpc.h"

namespace fbase {
namespace dataserver {
namespace net {

const std::array<uint8_t, 4> ProtocolRPC::kMagicV1 = {0x23, 0x23, 0x23, 0x23};
const std::array<uint8_t, 4> ProtocolRPC::kMagicV2 = {0xFB, 0xFB, 0xFB, 0xFB};

bool ProtocolRPC::Match(const std::array<uint8_t, 4>& preface) {
    return preface == kMagicV1 || preface == kMagicV2;
}

void ProtocolRPC::OnDataArrived(const uint8_t* buf, size_t len) {
    // TODO:
}

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
