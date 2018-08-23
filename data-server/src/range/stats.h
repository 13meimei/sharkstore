_Pragma("once");

#include "monitor/statistics.h"

namespace sharkstore {
namespace dataserver {
namespace range {

class RangeStats {
public:
    RangeStats() = default;
    virtual ~RangeStats() = default;

    virtual void PushTime(monitor::HistogramType type, int64_t time) {}
    virtual void IncrLeaderCount() {}
    virtual void DecrLeaderCount() {}
    virtual void IncrSplitCount() {}
    virtual void DecrSplitCount() {}
};

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
