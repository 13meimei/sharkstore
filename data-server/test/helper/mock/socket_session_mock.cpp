#include "socket_session_mock.h"

#include <assert.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "common/ds_proto.h"
#include "frame/sf_logger.h"

bool SocketSessionMock::GetResult(google::protobuf::Message *req) {
    if (result_.empty()) {
        return false;
    }
    google::protobuf::io::ArrayInputStream input(result_.data(), result_.size());
    bool ret = req->ParseFromZeroCopyStream(&input);
    if (!ret) {
        return false;
    } else {
        result_.clear();
        return true;
    }
}

void SocketSessionMock::Send(ProtoMessage *msg, google::protobuf::Message *resp) {
    resp->SerializeToString(&result_);
    delete msg;
    delete resp;
}

