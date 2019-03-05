//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_WRITE_BATCH_H
#define SHARKSTORE_DS_WRITE_BATCH_H

#include "storage/write_batch_interface.h"
#include <rocksdb/write_batch.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

class RocksWriteBatch: public WriteBatchInterface {
public:
    RocksWriteBatch() = default;
    ~RocksWriteBatch() = default;

    Status Put(const std::string& key, const std::string& value);
    Status Put(void* column_family, const std::string& key, const std::string& value);
    Status Delete(const std::string& key);
    Status Delete(void* column_family, const std::string& key);

public:
    rocksdb::WriteBatch* getBatch() { return &batch_; }

private:
    rocksdb::WriteBatch batch_;
};

}}}

#endif //SHARKSTORE_DS_WRITE_BATCH_H
