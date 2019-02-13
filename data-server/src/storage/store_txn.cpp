#include "store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

// txn column family name
static const std::string kTxnCFName = "txn";

Status Store::getTxnLock(const std::string&key, txnpb::TxnValue* value) {
    return Status(Status::kNotSupported);
}

void Store::TxnPrepare(const txnpb::PrepareRequest& req, txnpb::PrepareResponse* resp) {
}

void Store::TxnDecide(const txnpb::DecideRequest& req, txnpb::DecideResponse* resp) {
}

void Store::TxnClearup(const txnpb::ClearupRequest& req, txnpb::ClearupResponse* resp) {
}

void Store::TxnGetLockInfo(const txnpb::GetLockInfoRequest& req, txnpb::GetLockInfoResponse* resp) {
}

void Store::TxnSelect(const txnpb::SelectRequest& req, txnpb::SelectResponse* resp) {
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
