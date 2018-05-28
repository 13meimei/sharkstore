_Pragma("once");

namespace fbase {
namespace dataserver {
namespace net {

struct RPCHeader {
    std::array<uint8_t, 4> magic;
    uint16_t version = 0;
    uint16_t msg_type = 0;
    uint16_t func_id = 0;
    uint64_t msg_id = 0;
    uint8_t stream_hash = 0;
    uint8_t proto_type = 0;
    uint32_t timeout = 0;
    uint32_t body_length = 0;
} __attribute__((packed));

static constexpr int kRPCHanlderSize = sizeof(RPCHeader);

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
