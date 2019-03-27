_Pragma("once");

#include <rocksdb/db.h>
#include <rocksdb/utilities/blob_db/blob_db.h>

#include "storage/db/db_interface.h"
#include "iterator.h"
#include "write_batch.h"
#include "common/ds_config.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RocksDBImpl: public DbInterface {
public:
    explicit RocksDBImpl(const rocksdb_config_t& config);
    RocksDBImpl(const rocksdb::Options&ops, const std::string& path);

    ~RocksDBImpl();

public:
    bool IsInMemory() override { return false; }

    Status Open() override;

    Status Get(const std::string& key, std::string* value) override;
    Status Get(void* column_family, const std::string& key, std::string* value) override;
    Status Put(const std::string& key, const std::string& value) override;
    Status Put(void* column_family, const std::string& key, const std::string& value) override;
    Status Delete(const std::string& key) override;
    Status Delete(void* column_family, const std::string& key) override;
    Status DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) override;

    std::unique_ptr<WriteBatchInterface> NewBatch() override;
    Status Write(WriteBatchInterface* batch) override;

    void* DefaultColumnFamily() override;
    void* TxnCFHandle() override;

    IteratorInterface* NewIterator(const std::string& start, const std::string& limit) override;
    Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                        std::unique_ptr<IteratorInterface>& txn_iter,
                        const std::string& start, const std::string& limit) override;

    void GetProperty(const std::string& k, std::string* v) override;
    Status SetOptions(void* column_family, const std::unordered_map<std::string, std::string>& new_options) override;
    Status SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) override;

    void PrintMetric() override;
    void Scrub() override {}

public:
    Status CompactRange(const rocksdb::CompactRangeOptions& ops,
            rocksdb::Slice* start, rocksdb::Slice* end);

    Status Flush(const rocksdb::FlushOptions& ops);

private:
    void buildDBOptions(const rocksdb_config_t& config);
    void buildBlobOptions(const rocksdb_config_t& config);

private:
    const std::string db_path_;
    const bool is_blob_ = false;
    const int ttl_ = 0;
    rocksdb::Options ops_;
    rocksdb::blob_db::BlobDBOptions bops_;

    rocksdb::DB* db_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;
    rocksdb::ColumnFamilyHandle* txn_cf_ = nullptr;

    std::shared_ptr<rocksdb::Cache> block_cache_;  // rocksdb block cache
    std::shared_ptr<rocksdb::Cache> row_cache_; // rocksdb row cache
    std::shared_ptr<rocksdb::Statistics> db_stats_; // rocksdb stats
};

} // namespace storage
} // namespace dataserver
} // namespace sharkstore
