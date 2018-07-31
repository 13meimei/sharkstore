//
// Created by 丁俊 on 2018/7/30.
//

#include "master_server.h"

#ifndef LOG_MACRO_FNAME_
#define LOG_MACRO_FNAME_
#define __FNAME__ "master_server"
#endif

#include <frame/sf_logger.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
static const std::string addr = "127.0.0.1:7080";
int mspb::startMSRPC() {
    std::string server_address(addr);
    mspb::master_server service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    if(g_log_context.log_buff != nullptr) {
        FLOG_INFO("server listening on %s", server_address.c_str());
    }
    server->Wait();
    return 0;
}


::grpc::Status mspb::master_server::NodeLogin(::grpc::ServerContext *context, const ::mspb::NodeLoginRequest *request,
                                              ::mspb::NodeLoginResponse *response) {
    if(g_log_context.log_buff != nullptr) {
        FLOG_INFO("NodeLogin %"PRId64, request->node_id());
    }
    return ::grpc::Status(::grpc::StatusCode::OK, "");
}

::grpc::Status mspb::master_server::GetNodeId(::grpc::ServerContext *context, const ::mspb::GetNodeIdRequest *request,
                                              ::mspb::GetNodeIdResponse *response) {
    response->set_node_id(1);
    response->set_clearup(false);
    if(g_log_context.log_buff != nullptr) {
        FLOG_INFO("get node id %u", request->raft_port());
    }
    return ::grpc::Status(::grpc::StatusCode::OK, "");
}

::grpc::Status
mspb::master_server::GetMSLeader(::grpc::ServerContext *context, const ::mspb::GetMSLeaderRequest *request,
                                 ::mspb::GetMSLeaderResponse *response) {
    response->mutable_leader()->set_address(addr);
    if(g_log_context.log_buff != nullptr){
        FLOG_INFO("get ms leader %s",response->leader().address().c_str());
    }

    return ::grpc::Status(::grpc::StatusCode::OK, "");
}
