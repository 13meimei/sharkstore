#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>

#include <fastcommon/logger.h>
#include <fastcommon/process_ctrl.h>
#include <fastcommon/pthread_func.h>
#include <fastcommon/sched_thread.h>
#include <fastcommon/shared_func.h>

#include "common/ds_config.h"
#include "frame/sf_service.h"

#include "version.h"
#include "server.h"

using namespace sharkstore::dataserver::server;

int ds_user_init_callback() { return DataServer::Instance().Start(); }
void ds_user_destroy_callback() { DataServer::Instance().Stop(); }

int main(int argc, char *argv[]) {
    // print version
    if (argc > 1 && (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0)) {
        std::cout << GetVersionInfo() << std::endl;
        return 0;
    }

    sf_regist_load_config_callback(load_from_conf_file);
    sf_regist_user_init_callback(ds_user_init_callback);
    sf_regist_user_destroy_callback(ds_user_destroy_callback);

    sf_service_run(argc, argv, "data_server");

    return 0;
}
