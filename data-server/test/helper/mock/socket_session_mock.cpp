#include "socket_session_mock.h"

#include <assert.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include "common/ds_proto.h"
#include "frame/sf_logger.h"

ProtoMessage *SocketSessionMock::GetProtoMessage(const void *data) {
    assert(data != nullptr);

    ProtoMessage *msg = new ProtoMessage;
    if (msg == nullptr) return nullptr;
    
    // 解析头部
    ds_proto_header_t *proto_header = (ds_proto_header_t *)(data);

    ds_unserialize_header(proto_header, &(msg->header));

    // 拷贝数据
    if (msg->header.body_len > 0) {
        msg->body.resize(msg->header.body_len);
        memcpy(msg->body.data(), (char *)data + header_size, msg->body.size());
    }

    return msg;
}

bool SocketSessionMock::GetMessage(const char *data, size_t size,
                                   google::protobuf::Message *req) {
    google::protobuf::io::ArrayInputStream input(data, size);
    return req->ParseFromZeroCopyStream(&input);
}

bool SocketSessionMock::GetResult(google::protobuf::Message *req) {
    google::protobuf::io::ArrayInputStream input(result_.data(), result_.size());
    return req->ParseFromZeroCopyStream(&input);
}

void SocketSessionMock::Send(ProtoMessage *msg, google::protobuf::Message *resp) {
    resp->SerializeToString(&result_);
    delete msg;
    delete resp;
}

void SocketSessionMock::SetResponseHeader(const kvrpcpb::RequestHeader &req,
                                          kvrpcpb::ResponseHeader *resp,
                                          errorpb::Error *err) {
    // TODO
    // set timestamp
    resp->set_cluster_id(req.cluster_id());
    resp->set_trace_id(req.trace_id());

    if (err != nullptr) {
        resp->set_allocated_error(err);
    }
}
