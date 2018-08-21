_Pragma("once");

#include <string>
#include <cstdint>
#include <memory>

namespace sharkstore {
namespace dataserver {
namespace range {

enum class SplitKeyType {
    kNormal = 0,
    kKeepFirstPart, // 保持key第一部分相同的不分开
};

std::string SplitKeyTypeName(SplitKeyType type);

class SplitPolicy {
public:
    virtual ~SplitPolicy() = default;

    virtual std::string Name() const = 0;

    virtual bool Enabled() const = 0;

    virtual uint64_t CheckSize() const = 0;
    virtual uint64_t SplitSize() const = 0;
    virtual uint64_t MaxSize() const = 0;

    virtual SplitKeyType GetSplitKeyType() = 0;
};

std::unique_ptr<SplitPolicy> NewDisableSplitPolicy();

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
