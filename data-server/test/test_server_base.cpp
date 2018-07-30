//
// Created by 丁俊 on 2018/7/26.
//

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

#include <fastcommon/ini_file_reader.h>
#include <fastcommon/logger.h>
#include <fastcommon/process_ctrl.h>
#include <fastcommon/pthread_func.h>
#include <fastcommon/sched_thread.h>
#include <fastcommon/shared_func.h>


#include "common/ds_config.h"
#include "common/ds_proto.h"
#include "common/ds_types.h"
#include "common/ds_version.h"
#include "frame/sf_service.h"
#include "frame/sf_config.h"
#include "frame/sf_logger.h"

#include "server/callback.h"
#include "server/version.h"
#include "server/server.h"

#include "ds_conf.h"

#include <fastcommon/logger.h>

#include "common/socket_server.h"
#include "common/socket_session_impl.h"
#include "frame/sf_service.h"
#include "frame/sf_socket_buff.h"
#include "frame/sf_status.h"
#include "frame/sf_util.h"
#include <thread>
#include "mock/master_server.h"

using sharkstore::dataserver::server::GetVersionInfo;



int main() {
    std::thread ms(mspb::startMSRPC);
    sf_set_proto_header_size(sizeof(ds_proto_header_t));
    sf_regist_body_length_callback(ds_get_body_length);
    sf_regist_load_config_callback(load_from_conf_file);

    sf_regist_print_version_callback(print_version);

    sf_regist_user_init_callback(ds_user_init_callback);
    sf_regist_user_destroy_callback(ds_user_destroy_callback);

    sf_service_run_test(confStr.c_str());
    std::cout<<"prepare ms detach"<<std::endl;
    ms.detach();
    return 0;
}


