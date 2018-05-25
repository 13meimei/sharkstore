_Pragma("once");

namespace fbase {
namespace dataserver {
namespace net {

class Handler {
public:
    Handler() {}
    virtual ~Handler() {}

    virtual bool HandleRPC() = 0;
    virtual bool HandleTelnet(const std::vector<std::string>& args) = 0;
};

}  // namespace net
}  // namespace dataserver
}  // namespace fbase
