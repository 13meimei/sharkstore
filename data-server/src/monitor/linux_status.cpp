#include "linux_status.h"

#include <sys/vfs.h>
#include <string.h>

namespace sharkstore {
namespace monitor {

bool LinuxStatus::GetFileSystemUsage(const char *path, uint64_t *total, uint64_t *available) {
    struct statfs buf;
    memset(&buf, 0, sizeof(buf));
    int ret = ::statfs(path, &buf);
    if (ret == 0) {
        *total = buf.f_bsize * buf.f_blocks;
        *available = buf.f_bsize * buf.f_bavail;
        return true;
    } else {
        return false;
    }
}

bool LinuxStatus::GetMemoryUsage(uint64_t *total, uint64_t *available) {
    // TODO:
    return false;
}

}
}
