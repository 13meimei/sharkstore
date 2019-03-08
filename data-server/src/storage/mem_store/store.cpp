#include "store.h"
#include "storage/write_batch_interface.h"
#include "write_batch.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Status MemStore::Get(const std::string &key, std::string *value) {
    auto s = db_.Get(key, value);
    return Status(static_cast<Status::Code>(s));
}

Status MemStore::Get(void* column_family,
                       const std::string& key, std::string* value) {
    auto cf = static_cast<ColumnFamily *>(column_family);
    auto s = cf->Get(key, value);
    return Status(static_cast<Status::Code>(s));
}

Status MemStore::Put(const std::string &key, const std::string &value) {
    auto s = db_.Put(key, value);
    return Status(static_cast<Status::Code>(s));
}

Status MemStore::Write(WriteBatchInterface* batch) {
    // todo write batch
    return Status(Status::kOk);
}

Status MemStore::Delete(const std::string &key) {
    auto s = db_.Delete(key);
    return Status(static_cast<Status::Code>(s));
}

Status MemStore::Delete(void* column_family, const std::string& key) {
    auto cf = static_cast<ColumnFamily *>(column_family);
    auto s = cf->Delete(key);
    return Status(static_cast<Status::Code>(s));
}

Status MemStore::DeleteRange(void *column_family,
                               const std::string &begin_key, const std::string &end_key) {
    auto cf = static_cast<ColumnFamily *>(column_family);
    auto s = cf->DeleteRange(begin_key, end_key);
    return Status(static_cast<Status::Code>(s));
}

void* MemStore::DefaultColumnFamily() {
    return &db_;
}

void* MemStore::TxnCFHandle() {
    return &txn_cf_;
}

IteratorInterface* MemStore::NewIterator(const std::string& start, const std::string& limit) {
    auto it = db_.NewIterator(start, limit);
    return new MemIterator(it);
}

Status MemStore::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                    std::unique_ptr<IteratorInterface>& txn_iter,
                    const std::string& start, const std::string& limit) {
    return Status(Status::Code::kOk);
}

void MemStore::GetProperty(const std::string& k, std::string* v) {
    return;
}

std::unique_ptr<WriteBatchInterface> MemStore::NewBatch() {
    return std::unique_ptr<WriteBatchInterface>(new MemWriteBatch(this));
}

Status MemStore::SetOptions(void* column_family,
                              const std::unordered_map<std::string, std::string>& new_options) {
    return Status(Status::kOk);
}

Status MemStore::SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) {
    return Status(Status::kOk);
}

Status MemStore::CompactRange(void* options,
                                void* begin, void* end) {
    return Status(Status::kOk);
}

Status MemStore::Flush(void* fops) {
    return Status(Status::kOk);
}

void MemStore::PrintMetric() {

}

}}}
