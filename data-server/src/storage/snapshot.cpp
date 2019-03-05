#include "snapshot.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Snapshot::Snapshot(uint64_t applied, std::string&& context,
        std::unique_ptr<IteratorInterface> data_iter, std::unique_ptr<IteratorInterface> txn_iter) :
        applied_(applied),
        context_(std::move(context)),
        data_iter_(std::move(data_iter)),
        txn_iter_(std::move(txn_iter)) {
}

Snapshot::~Snapshot() {
    Close();
}

Status Snapshot::Context(std::string* context) {
    *context = context_;
    return Status::OK();
}

Status Snapshot::Next(std::string* data, bool* over) {
    if (!data_over_) {
        auto s = next(data_iter_.get(), raft_cmdpb::CF_DEFAULT, data, &data_over_);
        if (!s.ok()) {
            return s;
        }
        if (!data_over_) { // get data in default cf
            return s;
        }
    }
    // data_over_ == true
    return next(txn_iter_.get(), raft_cmdpb::CF_TXN, data, over);
}


Status Snapshot::next(IteratorInterface* iter, raft_cmdpb::CFType cf_type, std::string* data, bool *over) {
    if (iter->Valid()) {
        raft_cmdpb::SnapshotKVPair p;
        p.set_cf_type(cf_type);
        p.set_key(iter->key());
        p.set_value(iter->value());
        if (!p.SerializeToString(data)) {
            return Status(Status::kCorruption, "serialize snapshot data", "pb return false");
        }
        *over = false;
        iter->Next();
    } else {
        *over = true;
    }
    return iter->status();
}

void Snapshot::Close() {
    data_iter_.reset();
    txn_iter_.reset();
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */

