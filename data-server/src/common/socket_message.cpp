#include "socket_message.h"

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace sharkstore {
namespace dataserver {
namespace common {

bool GetMessage(const char *data, size_t size,
                google::protobuf::Message *req, bool zero_copy) {
    if (zero_copy) {
        google::protobuf::io::ArrayInputStream input(data, static_cast<int>(size));
        return req->ParseFromZeroCopyStream(&input);
    } else {
        return req->ParseFromArray(data, static_cast<int>(size));
    }
}

void SetResponseHeader(const kvrpcpb::RequestHeader &req,
                       kvrpcpb::ResponseHeader *resp,
                       errorpb::Error *err) {
    resp->set_cluster_id(req.cluster_id());
    resp->set_trace_id(req.trace_id());
    if (err != nullptr) {
        resp->set_allocated_error(err);
    }
}

}  // namespace common
}  // namespace dataserver
}  // namespace sharkstore
