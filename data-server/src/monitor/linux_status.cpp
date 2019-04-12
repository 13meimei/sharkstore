#include "linux_status.h"

#include <sys/vfs.h>
#include <stdio.h>
#include <sys/sysinfo.h>
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
    const char* kTotalName = "MemTotal";
    const char* kAvailableName = "MemAvailable";

    FILE* fp = ::fopen("/proc/meminfo", "r");
    if (fp == NULL) {
        perror("open /proc/meminfo");
        return false;
    }

    bool find_total = false, find_avail = false;
    int ret = 0;
    while (!find_total || !find_avail) {
        char name[256] = {'\0'};
        unsigned long val = 0;
        ret = fscanf(fp, "%s %lu", name, &val);
        if (ret <= 0) {
            if (ret < 0) {
                perror("fscanf /pro/meminfo");
            }
            break;
        }
        if (strncmp(name, kTotalName, strlen(kTotalName)) == 0) {
            *total = val;
            find_total = true;
        } else if (strncmp(name, kAvailableName, strlen(kAvailableName)) == 0) {
            *available = val;
            find_avail = true;
        }
    }
    ::fclose(fp);
    return find_total && find_avail;
}

}
}
