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
                       const std::string& key, void* value) {
    // todo column family
    return Status(Status::kInvalid);
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
    // todo
    return Status(Status::kOk);
}

Status MemStore::DeleteRange(void *column_family,
                               const std::string &begin_key, const std::string &end_key) {
    auto s = db_.DeleteRange(begin_key, end_key);
    return Status(static_cast<Status::Code>(s));
}

void* MemStore::DefaultColumnFamily() {
    return nullptr;
}

void* MemStore::TxnCFHandle() {
    return nullptr;
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

Status MemStore::Insert(storage::Store* store,
                          const kvrpcpb::InsertRequest& req, uint64_t* affected) {
    // todo batch
    uint64_t bytes_written = 0;
//    MemWriteBatch batch(this);
    std::string value;
    bool check_dup = req.check_duplicate();
    *affected = 0;
    for (int i = 0; i < req.rows_size(); ++i) {
        const kvrpcpb::KeyValue &kv = req.rows(i);
        if (check_dup) {
            auto s = db_.Get(kv.key(), &value);
            if (s == 0) {
                return Status(Status::kDuplicate);
            }
        }
//        auto s = batch.Put(kv.key(), kv.value());
        auto s = db_.Put(kv.key(), kv.value());
        if (s != 0) {
            return Status(Status::kIOError);
        }
        *affected = *affected + 1;
        bytes_written += (kv.key().size(), kv.value().size());
    }
//    auto s = db_->Write(&batch);
//    if (s != 0) {
//        return Status(Status::kIOError, "batch write", s.ToString());
//    } else {
        store->addMetricWrite(*affected, bytes_written);
        return Status::OK();
//    }
}

WriteBatchInterface* MemStore::NewBatch() {
    return new MemWriteBatch(this);
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
