//
// Created by young on 19-2-14.
//

#include "write_batch.h"
#include <rocksdb/write_batch.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

Status RocksWriteBatch::Put(const std::string &key, const std::string &value) {
    auto s = batch_.Put(key, value);
    return Status(static_cast<Status::Code >(s.code()));
}

Status RocksWriteBatch::Put(void* column_family, const std::string& key, const std::string& value) {
    auto s = batch_.Put(static_cast<rocksdb::ColumnFamilyHandle*>(column_family), key, value);
    return Status(static_cast<Status::Code >(s.code()));
}

Status RocksWriteBatch::Delete(const std::string &key) {
    auto s = batch_.Delete(key);
    return Status(static_cast<Status::Code >(s.code()));
}

Status RocksWriteBatch::Delete(void* column_family, const std::string& key) {
    auto s = batch_.Delete(static_cast<rocksdb::ColumnFamilyHandle*>(column_family), key);
    return Status(static_cast<Status::Code >(s.code()));
}

}}}
