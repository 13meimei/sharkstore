#include "ds_version.h"

#include <stdio.h>

Version g_ds_version = {1, 1, 50};

char version[32];

void print_version() {
    fprintf(stderr, "ds version %d.%d.%d\n",
            g_ds_version.major, g_ds_version.minor, g_ds_version.patch);

}

char *get_version() {
    sprintf(version, "%d.%d.%d",
            g_ds_version.major, g_ds_version.minor, g_ds_version.patch);

    return version;
}
