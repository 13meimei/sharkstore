#include "rpc_protocol.h"

#include "base/byte_order.h"

namespace sharkstore {
namespace dataserver {
namespace net {

bool RPCHead::Valid() const {
    bool valid_magic = std::equal(magic, magic + 4, kRPCMagicV1.cbegin()) ||
                       std::equal(magic, magic + 4, kRPCMagicV2.cbegin());
    if (!valid_magic) {
        return false;
    }
    if (body_length > kMaxRPCBodyLength) {
        return false;
    }
    return true;
}

void RPCHead::Encode() {
    version = htobe16(version);
    msg_type = htobe16(msg_type);
    func_id = htobe16(func_id);
    msg_id = htobe64(msg_id);
    timeout = htobe32(timeout);
    body_length = htobe32(body_length);
}

void RPCHead::Decode() {
    version = be16toh(version);
    msg_type = be16toh(msg_type);
    func_id = be16toh(func_id);
    msg_id = be64toh(msg_id);
    timeout = be32toh(timeout);
    body_length = be32toh(body_length);
}

std::string RPCHead::DebugString() const {
    // TODO:
    return "";
}

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
