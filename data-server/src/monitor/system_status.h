_Pragma("once");

#include <stdint.h>
#include <memory>

namespace sharkstore {
namespace monitor {

class SystemStatus {
public:
    virtual ~SystemStatus() = default;

    static std::unique_ptr<SystemStatus> New();

    virtual bool GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) = 0;
    virtual bool GetMemoryUsage(uint64_t *total, uint64_t *available) = 0;
};

} // namespace monitor
} // namespace sharkstore



