_Pragma("once");

#include "protocol.h"

namespace sharkstore {
namespace dataserver {
namespace net {

class ProtocolTelnet : public Protocol {
public:
    using HandlerType = std::function<void(const std::vector<std::string>& args)>;

public:
    explicit ProtocolTelnet(const HandlerType& handler);
    ~ProtocolTelnet();

    bool Match(const std::array<uint8_t, 4>& preface) override;
    void OnDataArrived(const uint8_t* buf, size_t len) override;
};

}  // namespace net
}  // namespace dataserver
}  // namespace sharkstore
