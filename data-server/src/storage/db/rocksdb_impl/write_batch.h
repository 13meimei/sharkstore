//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_WRITE_BATCH_H
#define SHARKSTORE_DS_WRITE_BATCH_H

#include <rocksdb/write_batch.h>
#include "storage/db/write_batch_interface.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RocksWriteBatch: public WriteBatchInterface {
public:
    RocksWriteBatch() = default;
    ~RocksWriteBatch() = default;

    Status Put(const std::string& key, const std::string& value) override;
    Status Put(void* column_family, const std::string& key, const std::string& value) override;
    Status Delete(const std::string& key) override;
    Status Delete(void* column_family, const std::string& key) override;

public:
    rocksdb::WriteBatch* getBatch() { return &batch_; }

private:
    rocksdb::WriteBatch batch_;
};

}}}

#endif //SHARKSTORE_DS_WRITE_BATCH_H
