//
// Created by 丁俊 on 2018/7/30.
//

#ifndef SHARKSTORE_DS_MASTER_SERVER_H
#define SHARKSTORE_DS_MASTER_SERVER_H
#include <grpc++/grpc++.h>
#include <grpc/grpc.h>
#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>

#include "proto/gen/mspb.grpc.pb.h"

namespace mspb{
    class master_server:public MsServer::Service {
        ::grpc::Status NodeLogin(::grpc::ServerContext* context, const ::mspb::NodeLoginRequest* request, ::mspb::NodeLoginResponse* response);

        ::grpc::Status GetNodeId(::grpc::ServerContext* context, const ::mspb::GetNodeIdRequest* request, ::mspb::GetNodeIdResponse* response);

        ::grpc::Status GetMSLeader(::grpc::ServerContext* context, const ::mspb::GetMSLeaderRequest* request, ::mspb::GetMSLeaderResponse* response);
    };

    int startMSRPC();
}



#endif //SHARKSTORE_DS_MASTER_SERVER_H
