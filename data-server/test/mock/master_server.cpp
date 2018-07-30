//
// Created by 丁俊 on 2018/7/30.
//

#include "master_server.h"


using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

int mspb::startMSRPC(){
    std::string server_address("0.0.0.0:7080");
    mspb::master_server service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
    return 0;
}



    ::grpc::Status mspb::master_server::NodeLogin(::grpc::ServerContext* context, const ::mspb::NodeLoginRequest* request, ::mspb::NodeLoginResponse* response){
        return ::grpc::Status(::grpc::StatusCode::OK, "");
    }

    ::grpc::Status mspb::master_server::GetNodeId(::grpc::ServerContext* context, const ::mspb::GetNodeIdRequest* request, ::mspb::GetNodeIdResponse* response){
        response->set_node_id(1);
        response->set_clearup(false);
        return ::grpc::Status(::grpc::StatusCode::OK, "");
    }

    ::grpc::Status mspb::master_server::GetMSLeader(::grpc::ServerContext* context, const ::mspb::GetMSLeaderRequest* request, ::mspb::GetMSLeaderResponse* response){
        mspb::MSLeader leader;
        leader.set_address("127.0.0.1:7080");
        response->set_allocated_leader(&leader);
        return ::grpc::Status(::grpc::StatusCode::OK, "");
    }
