#include "skiplist_impl.h"

#include "storage/db/memdb_batch.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Status SkipListDBImpl::Get(const std::string &key, std::string *value) {
    auto s = db_.Get(key, value);
    return Status(static_cast<Status::Code>(s));
}

Status SkipListDBImpl::Get(void* column_family,
                       const std::string& key, std::string* value) {
    auto cf = static_cast<ColumnFamily *>(column_family);
    auto s = cf->Get(key, value);
    return Status(static_cast<Status::Code>(s));
}

Status SkipListDBImpl::Put(const std::string &key, const std::string &value) {
    auto s = db_.Put(key, value);
    return Status(static_cast<Status::Code>(s));
}

Status SkipListDBImpl::Put(void* column_family, const std::string& key, const std::string& value) {
    auto cf = static_cast<ColumnFamily *>(column_family);
    auto s = cf->Put(key, value);
    return Status(static_cast<Status::Code>(s));
}

Status SkipListDBImpl::Write(WriteBatchInterface* batch) {
    auto batch_impl = dynamic_cast<MemDBWriteBatch*>(batch);
    if (batch_impl != nullptr) {
        return batch_impl->WriteTo(this);
    } else {
        return Status(Status::kInvalidArgument, "Write", "invalid batch");
    }
}

Status SkipListDBImpl::Delete(const std::string &key) {
    auto s = db_.Delete(key);
    return Status(static_cast<Status::Code>(s));
}

Status SkipListDBImpl::Delete(void* column_family, const std::string& key) {
    auto cf = static_cast<ColumnFamily *>(column_family);
    auto s = cf->Delete(key);
    return Status(static_cast<Status::Code>(s));
}

Status SkipListDBImpl::DeleteRange(void *column_family,
                               const std::string &begin_key, const std::string &end_key) {
    auto cf = static_cast<ColumnFamily *>(column_family);
    auto s = cf->DeleteRange(begin_key, end_key);
    if (s >= 0) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "DeleteRange", std::to_string(s));
    }
}

void* SkipListDBImpl::DefaultColumnFamily() {
    return &db_;
}

void* SkipListDBImpl::TxnCFHandle() {
    return &txn_cf_;
}

IteratorInterface* SkipListDBImpl::NewIterator(const std::string& start, const std::string& limit) {
    auto it = db_.NewIterator(start, limit);
    return new MemIterator(it);
}

Status SkipListDBImpl::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                    std::unique_ptr<IteratorInterface>& txn_iter,
                    const std::string& start, const std::string& limit) {
    return Status(Status::Code::kOk);
}

void SkipListDBImpl::GetProperty(const std::string& k, std::string* v) {
    return;
}

std::unique_ptr<WriteBatchInterface> SkipListDBImpl::NewBatch() {
    return std::unique_ptr<WriteBatchInterface>(new MemDBWriteBatch());
}

Status SkipListDBImpl::SetOptions(void* column_family,
                              const std::unordered_map<std::string, std::string>& new_options) {
    return Status(Status::kOk);
}

Status SkipListDBImpl::SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) {
    return Status(Status::kOk);
}

std::string SkipListDBImpl::GetMetrics(bool) { return ""; }

}
}
}
