_Pragma("once");

#include "system_status.h"

namespace sharkstore {
namespace monitor {

class LinuxStatus : public SystemStatus {
public:
    bool GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) override;
    bool GetMemoryUsage(uint64_t *total, uint64_t *available) override;
};

}
}