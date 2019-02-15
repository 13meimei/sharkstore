//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_STORE_H
#define SHARKSTORE_DS_STORE_H

#include "storage/db_interface.h"
#include <rocksdb/db.h>
#include "proto/gen/kvrpcpb.pb.h"
#include "iterator.h"
#include "write_batch.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RocksStore: public DbInterface {
public:
    RocksStore() = delete;
    RocksStore(rocksdb::DB* db, const rocksdb::ReadOptions& read_options,
            const rocksdb::WriteOptions& write_options);
    ~RocksStore() = default;

public:
    Status Get(const std::string& key, std::string* value);
    Status Get(void* column_family,
               const std::string& key, void* value);
    Status Put(const std::string& key, const std::string& value);
    Status Write(WriteBatchInterface* batch);
    Status Delete(const std::string& key);
    Status DeleteRange(void* column_family,
                       const std::string& begin_key, const std::string& end_key);
    void* DefaultColumnFamily();
    IteratorInterface* NewIterator(const std::string& start, const std::string& limit);
    void GetProperty(const std::string& k, std::string* v);

public:
    Status SetOptions(void* column_family,
                      const std::unordered_map<std::string, std::string>& new_options);
    Status SetDBOptions(const std::unordered_map<std::string, std::string>& new_options);
    Status CompactRange(void* options, void* begin, void* end);
    Status Flush(void* fops);

    Status Insert(storage::Store* store,
                  const kvrpcpb::InsertRequest& req, uint64_t* affected);
    WriteBatchInterface* NewBatch();

private:
    rocksdb::DB* db_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;
};


}}}

#endif //SHARKSTORE_DS_STORE_H
