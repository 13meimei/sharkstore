#include "socket_session_mock.h"

#include <assert.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "common/ds_proto.h"
#include "frame/sf_logger.h"

bool SocketSessionMock::GetResult(google::protobuf::Message *req) {
    google::protobuf::io::ArrayInputStream input(result_.data(), result_.size());
    return req->ParseFromZeroCopyStream(&input);
}

void SocketSessionMock::Send(ProtoMessage *msg, google::protobuf::Message *resp) {
    resp->SerializeToString(&result_);
    delete msg;
    delete resp;
}

