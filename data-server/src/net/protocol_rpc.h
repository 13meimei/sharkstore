_Pragma("once");

#include "protocol.h"

namespace fbase {
namespace dataserver {
namespace net {

class ProtocolRPC : public Protocol {
public:
    ProtocolRPC();
    ~ProtocolRPC();

    bool Match(const std::array<uint8_t, 4>& preface) override;
    void OnDataArrived(const uint8_t* buf, size_t len) override;

private:
    // TODO: remove v1
    static const std::array<uint8_t, 4> kMagicV1;
    static const std::array<uint8_t, 4> kMagicV2;
};

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
