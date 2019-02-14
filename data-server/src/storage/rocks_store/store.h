//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_STORE_H
#define SHARKSTORE_DS_STORE_H

#include "storage/db_interface.h"
#include <rocksdb/db.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

class RocksStore: public DbInterface {
public:
    RocksStore(const rocksdb::ReadOptions& read_options,
            const rocksdb::WriteOptions& write_options) = default;
    ~RocksStore() = default;

public:
    Status Get(const std::string& key, std::string* value);
    Status Put(const std::string& key, const std::string& value);
    Status Write(WriteBatchInterface* batch);
    Status Delete(const std::string& batch);
    Status DeleteRange(void* column_family,
                       const std::string& begin_key, const std::string& end_key);
    void* DefaultColumnFamily();
    IteratorInterface* NewIterator(const std::string& start, const std::string& limit);

private:
    rocksdb::DB* db_;
    rocksdb::ReadOptions read_options_ const;
    rocksdb::WriteOptions write_options_ const;
};


}}}

#endif //SHARKSTORE_DS_STORE_H
