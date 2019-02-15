//
// Created by young on 19-2-14.
//

#include "store.h"
#include <rocksdb/utilities/blob_db/blob_db.h>
#include <common/ds_config.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

RocksStore::RocksStore(rocksdb::DB* db, const rocksdb::ReadOptions& read_options,
           const rocksdb::WriteOptions& write_options):
        db_(db), read_options_(read_options), write_options_(write_options) {

}

Status RocksStore::Get(const std::string &key, std::string *value) {
    auto s = db_->Get(read_options_, key, value);
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::Get(void* column_family,
           const std::string& key, void* value) {
    auto s = db_->Get(read_options_, static_cast<rocksdb::ColumnFamilyHandle*>(column_family),
                      key, static_cast<rocksdb::PinnableSlice*>(value));
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::Put(const std::string &key, const std::string &value) {
    rocksdb::Status s;

    if(ds_config.rocksdb_config.storage_type == 1 && ds_config.rocksdb_config.ttl > 0) {
        auto *blobdb = static_cast<rocksdb::blob_db::BlobDB*>(db_);
        s = blobdb->PutWithTTL(write_options_,rocksdb::Slice(key),rocksdb::Slice(value),ds_config.rocksdb_config.ttl);
    } else {
        s = db_->Put(write_options_, key, value);
    }
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::Write(WriteBatchInterface* batch) {
    auto s = db_->Write(write_options_, dynamic_cast<RocksWriteBatch*>(batch)->getBatch());
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::Delete(const std::string &key) {
    auto s = db_->Delete(write_options_, key);
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::DeleteRange(void *column_family,
                               const std::string &begin_key, const std::string &end_key) {
    auto s = db_->DeleteRange(write_options_,
                              static_cast<rocksdb::ColumnFamilyHandle*>(column_family),
                              begin_key, end_key);
    return Status(static_cast<Status::Code>(s.code()));
}

void* RocksStore::DefaultColumnFamily() {
    return db_->DefaultColumnFamily();
}

IteratorInterface* RocksStore::NewIterator(const std::string& start, const std::string& limit) {
    auto it = db_->NewIterator(read_options_);
    return new RocksIterator(it, start, limit);
}

void RocksStore::GetProperty(const std::string& k, std::string* v) {
    db_->GetProperty(k, v);
}

Status RocksStore::Insert(storage::Store* store,
                          const kvrpcpb::InsertRequest& req, uint64_t* affected) {
    if (ds_config.rocksdb_config.storage_type == 1 && ds_config.rocksdb_config.ttl > 0) {
        auto *blobdb = static_cast<rocksdb::blob_db::BlobDB *>(db_);
        std::string value;
        rocksdb::Status s;
        bool check_dup = req.check_duplicate();
        *affected = 0;
        for (int i = 0; i < req.rows_size(); ++i) {
            const kvrpcpb::KeyValue &kv = req.rows(i);
            if (check_dup) {
                s = db_->Get(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum, true), kv.key(), &value);
                if (s.ok()) {
                    return Status(Status::kDuplicate);
                } else if (!s.IsNotFound()) {
                    return Status(Status::kIOError, "get", s.ToString());
                }
            }
            s = blobdb->PutWithTTL(write_options_, rocksdb::Slice(kv.key()), rocksdb::Slice(kv.value()),
                                   ds_config.rocksdb_config.ttl);
            if (!s.ok()) {
                return Status(Status::kIOError, "blobdb put", s.ToString());
            } else {
                store->addMetricWrite(*affected, kv.key().size() + kv.value().size());
                *affected = *affected + 1;
            }

        }

        return Status::OK();

    }

    uint64_t bytes_written = 0;
    rocksdb::WriteBatch batch;
    rocksdb::Status s;
    std::string value;
    bool check_dup = req.check_duplicate();
    *affected = 0;
    for (int i = 0; i < req.rows_size(); ++i) {
        const kvrpcpb::KeyValue &kv = req.rows(i);
        if (check_dup) {
            s = db_->Get(rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum, true), kv.key(), &value);
            if (s.ok()) {
                return Status(Status::kDuplicate);
            } else if (!s.IsNotFound()) {
                return Status(Status::kIOError, "get", s.ToString());
            }
        }
        s = batch.Put(kv.key(), kv.value());
        if (!s.ok()) {
            return Status(Status::kIOError, "batch put", s.ToString());
        }
        *affected = *affected + 1;
        bytes_written += (kv.key().size(), kv.value().size());
    }
    s = db_->Write(write_options_, &batch);
    if (!s.ok()) {
        return Status(Status::kIOError, "batch write", s.ToString());
    } else {
        store->addMetricWrite(*affected, bytes_written);
        return Status::OK();
    }
}

WriteBatchInterface* RocksStore::NewBatch() {
    return new RocksWriteBatch;
}

Status RocksStore::SetOptions(void* column_family,
                              const std::unordered_map<std::string, std::string>& new_options) {
    auto s = db_->SetOptions(static_cast<rocksdb::ColumnFamilyHandle*>(column_family), new_options);
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) {
    auto s = db_->SetDBOptions(new_options);
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::CompactRange(void* options,
                         void* begin, void* end) {
    auto s= db_->CompactRange(*static_cast<rocksdb::CompactRangeOptions*>(options),
                              static_cast<rocksdb::Slice*>(begin),
                              static_cast<rocksdb::Slice*>(end));
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksStore::Flush(void* fops) {
    auto s = db_->Flush(*static_cast<rocksdb::FlushOptions*>(fops));
    return Status(static_cast<Status::Code>(s.code()));
}

}}}
