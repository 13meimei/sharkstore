_Pragma("once");

#include "protocol.h"

namespace fbase {
namespace dataserver {
namespace net {

class ProtocolTelnet : public Protocol {
public:
    ProtocolTelnet();
    ~ProtocolTelnet();

    bool Match(const std::array<uint8_t, 4>& preface) override;
    void OnDataArrived(const uint8_t* buf, size_t len) override;
};

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
