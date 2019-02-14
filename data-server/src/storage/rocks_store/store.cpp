//
// Created by young on 19-2-14.
//

#include "store.h"
#include "iterator.h"


namespace sharkstore {
namespace dataserver {
namespace storage {

//    write_options_.disableWAL = ds_config.rocksdb_config.disable_wal;
//rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true)

RocksStore::RocksStore(const rocksdb::ReadOptions& read_options,
           const rocksdb::WriteOptions& write_options):
        read_options_(read_options), write_options_(write_options) {

}

RocksStore::~RocksStore() {}

Status RocksStore::Get(const std::string &key, std::string *value) {
    auto s = db_->Get(read_options_, key, value);
    return Status(s.code());
}

Status RocksStore::Put(const std::string &key, const std::string &value) {
    auto s = db_->Put(write_options_, key, value));
    return Status(s.code());
}

Status RocksStore::Write(WriteBatchInterface *batch) {
    auto s = db_->Write(write_options_, batch);
    return Status(s.code());
}

Status RocksStore::Delete(const std::string &batch) {
    auto s = db_->Delete(write_options_, batch);
    return Status(s.code());
}

Status RocksStore::DeleteRange(void *column_family,
                               const std::string &begin_key, const std::string &end_key) {
    auto s = db_->DeleteRange(write_options_,
                              static_cast<rocksdb::ColumnFamilyHandle*>(column_family),
                              begin_key, end_key);
    return Status(s.code());
}

void* RocksStore::DefaultColumnFamily() {
    return db_->DefaultColumnFamily();
}

IteratorInterface* RocksStore::NewIterator(const std::string& start, const std::string& limit) {
    return new RocksIterator(read_options_, start, limit);
}

}}}
