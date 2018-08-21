#include "split_policy.h"

namespace sharkstore {
namespace dataserver {
namespace range {

std::string SplitKeyTypeName(SplitKeyType type) {
    switch (type) {
        case SplitKeyType::kNormal:
            return "normal";
        case SplitKeyType::kKeepFirstPart:
            return "keep first part";
        default:
            return "<unknown>";
    };
}

class DisableSplitPolicy : public SplitPolicy {
public:
    std::string Name() const override { return "DisableSplit"; }
    bool Enabled() const override { return false; }
    uint64_t CheckSize() const override { return 0; }
    uint64_t SplitSize() const override { return 0; }
    uint64_t MaxSize() const override { return 0; }
    SplitKeyType GetSplitKeyType() override { return SplitKeyType::kNormal; }
};

std::unique_ptr<SplitPolicy> NewDisableSplitPolicy() {
    return std::unique_ptr<SplitPolicy>(new DisableSplitPolicy);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
