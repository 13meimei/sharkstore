_Pragma("once");

#include "protocol.h"

namespace fbase {
namespace dataserver {
namespace net {

class ProtocolRPC : public Protocol {
public:
    using std::function<void(const std::vector<uint8_t>& buf)> HandlerType;

public:
    explicit ProtocolRPC(const HandlerType& handler);
    ~ProtocolRPC();

    bool Match(const std::array<uint8_t, 4>& preface) override;
    void OnDataArrived(const uint8_t* buf, size_t len) override;

    void SendResponse(const RPCHeader& header, const std::vector<uint8_t>& body);

private:
    // TODO: remove v1
    static const std::array<uint8_t, 4> kMagicV1;
    static const std::array<uint8_t, 4> kMagicV2;

    HandlerType handler;
};

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
