#include "system_status.h"

#ifdef __linux__
#include "linux_status.h"
#elif defined(__APPLE__)
#include "mac_status.h"
#else
#error unsupport platform
#endif

namespace sharkstore {
namespace monitor {

std::unique_ptr<SystemStatus> SystemStatus::New() {
#ifdef __linux__
    return std::unique_ptr<SystemStatus>(new LinuxStatus);
#elif defined(__APPLE__)
    return std::unique_ptr<SystemStatus>(new MacStatus);
#else
#error unsupport platform
#endif
}

} // namespace monitor
} // namespace sharkstore
