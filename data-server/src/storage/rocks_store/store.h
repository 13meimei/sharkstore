//
// Created by young on 19-2-14.
//

#ifndef SHARKSTORE_DS_STORE_H
#define SHARKSTORE_DS_STORE_H

#include "storage/db_interface.h"
#include <rocksdb/db.h>
#include <rocksdb/utilities/blob_db/blob_db.h>

#include "proto/gen/kvrpcpb.pb.h"
#include "iterator.h"
#include "write_batch.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RocksStore: public DbInterface {
public:
    RocksStore();
//    RocksStore(const rocksdb::ReadOptions& read_options,
//               const rocksdb::WriteOptions& write_options);
    ~RocksStore();

public:
    Status Get(const std::string& key, std::string* value);
    Status Get(void* column_family,
               const std::string& key, void* value);
    Status Put(const std::string& key, const std::string& value);
    Status Write(WriteBatchInterface* batch);
    Status Delete(const std::string& key);
    Status Delete(void* column_family, const std::string& key);
    Status DeleteRange(void* column_family,
                       const std::string& begin_key, const std::string& end_key);
    void* DefaultColumnFamily();
    void* TxnCFHandle();
    IteratorInterface* NewIterator(const std::string& start, const std::string& limit);
    Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                        std::unique_ptr<IteratorInterface>& txn_iter,
                        const std::string& start = "", const std::string& limit = "");
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
    void PrintMetric();

private:
    int openDB();
    void buildDBOptions(rocksdb::Options& ops);
    void buildBlobOptions(rocksdb::blob_db::BlobDBOptions& bops);

private:
    rocksdb::DB* db_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;
    rocksdb::ColumnFamilyHandle* txn_cf_;

    std::shared_ptr<rocksdb::Cache> block_cache_;  // rocksdb block cache
    std::shared_ptr<rocksdb::Cache> row_cache_; // rocksdb row cache
    std::shared_ptr<rocksdb::Statistics> db_stats_; // rocksdb stats
};

}}}

#endif //SHARKSTORE_DS_STORE_H
