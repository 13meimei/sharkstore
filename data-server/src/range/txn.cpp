#include "range.h"

namespace sharkstore {
namespace dataserver {
namespace range {

using namespace sharkstore::monitor;

static void setTxnServerErr(txnpb::TxnError* err, int32_t code, const std::string& msg) {
    err->set_err_type(txnpb::TxnError_ErrType_SERVER_ERROR);
    err->mutable_server_err()->set_code(code);
    err->mutable_server_err()->set_msg(msg);
}

void Range::TxnPrepare(common::ProtoMessage* msg, txnpb::DsPrepareRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("TxnPrepare begin");
}

Status Range::ApplyTxnPrepare(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    return Status(Status::kNotSupported);
}

void Range::TxnDecide(common::ProtoMessage* msg, txnpb::DsDecideRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("TxnDecide begin");
}

Status Range::ApplyTxnDecide(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    return Status(Status::kNotSupported);
}

void Range::TxnClearup(common::ProtoMessage* msg, txnpb::DsClearupRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);

    RANGE_LOG_DEBUG("TxnClearup begin");

}

Status Range::ApplyTxnClearup(const raft_cmdpb::Command &cmd, uint64_t raft_index) {
    return Status(Status::kNotSupported);
}

void Range::TxnGetLockInfo(common::ProtoMessage* msg, txnpb::DsGetLockInfoRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);
}

void Range::TxnSelect(common::ProtoMessage* msg, txnpb::DsSelectRequest& req) {
    auto btime = get_micro_second();
    context_->Statistics()->PushTime(HistogramType::kQWait, btime - msg->begin_time);
}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
