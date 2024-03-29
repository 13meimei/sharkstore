//
// Created by young on 19-2-14.
//

#include "rocksdb_impl.h"

#include <sstream>
#include <rocksdb/utilities/blob_db/blob_db.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/utilities/db_ttl.h>

#include "base/util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

static const std::string kDataPathSuffix = "data";
static const std::string kTxnCFName = "txn";

RocksDBImpl::RocksDBImpl(const rocksdb_config_t& config) :
    db_path_(JoinFilePath({config.path, kDataPathSuffix})),
    is_blob_(config.storage_type != 0),
    ttl_(config.ttl) {
    buildDBOptions(config);
    if (is_blob_) {
        buildBlobOptions(config);
    }
    if (config.disable_wal) {
        write_options_.disableWAL = true;
    }
    read_options_ = rocksdb::ReadOptions(config.read_checksum, true);
}

RocksDBImpl::RocksDBImpl(const rocksdb::Options&ops, const std::string& path) :
    db_path_(path),
    ops_(ops) {
}

void RocksDBImpl::buildDBOptions(const rocksdb_config_t& config) {
    // db log level
    if (config.enable_debug_log) {
        ops_.info_log_level = rocksdb::DEBUG_LEVEL;
    }

    // db stats
    if (config.enable_stats) {
        db_stats_ = rocksdb::CreateDBStatistics();;
        ops_.statistics = db_stats_;
    }

    // table options include block_size, block_cache_size, etc
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = config.block_size;
    if (config.block_cache_size > 0) {
        block_cache_ = rocksdb::NewLRUCache(config.block_cache_size);
        table_options.block_cache = block_cache_;
    }
    if (config.cache_index_and_filter_blocks){
        table_options.cache_index_and_filter_blocks = true;
    }
    ops_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // row_cache
    if (config.row_cache_size > 0){
        row_cache_ = rocksdb::NewLRUCache(config.row_cache_size);;
        ops_.row_cache = row_cache_;
    }

    ops_.max_open_files = config.max_open_files;
    ops_.create_if_missing = true;
    ops_.use_fsync = true;
    ops_.use_adaptive_mutex = true;
    ops_.bytes_per_sync = config.bytes_per_sync;

    // memtables
    ops_.write_buffer_size = config.write_buffer_size;
    ops_.max_write_buffer_number = config.max_write_buffer_number;
    ops_.min_write_buffer_number_to_merge = config.min_write_buffer_number_to_merge;

    // level & sst file size
    ops_.max_bytes_for_level_base = config.max_bytes_for_level_base;
    ops_.max_bytes_for_level_multiplier = config.max_bytes_for_level_multiplier;
    ops_.target_file_size_base = config.target_file_size_base;
    ops_.target_file_size_multiplier = config.target_file_size_multiplier;

    // compactions and flushes
    if (config.disable_auto_compactions) {
        ops_.disable_auto_compactions = true;
    }
    if (config.background_rate_limit > 0) {
        ops_.rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(
                rocksdb::NewGenericRateLimiter(static_cast<int64_t>(config.background_rate_limit)));
    }
    ops_.max_background_flushes = config.max_background_flushes;
    ops_.max_background_compactions = config.max_background_compactions;
    ops_.level0_file_num_compaction_trigger = config.level0_file_num_compaction_trigger;

    // write pause
    ops_.level0_slowdown_writes_trigger = config.level0_slowdown_writes_trigger;
    ops_.level0_stop_writes_trigger = config.level0_stop_writes_trigger;

    // compress
    auto compress_type = static_cast<rocksdb::CompressionType>(config.compression);
    switch (compress_type) {
        case rocksdb::kSnappyCompression: // 1
        case rocksdb::kZlibCompression:  // 2
        case rocksdb::kBZip2Compression: // 3
        case rocksdb::kLZ4Compression: // 4
        case rocksdb::kLZ4HCCompression: // 5
        case rocksdb::kXpressCompression: // 6
            ops_.compression = compress_type;
            break;
        default:
            ops_.compression = rocksdb::kNoCompression;
    }
}

void RocksDBImpl::buildBlobOptions(const rocksdb_config_t& config) {
    assert(config.min_blob_size >= 0);
    bops_.min_blob_size = static_cast<uint64_t>(config.min_blob_size);
    bops_.enable_garbage_collection = config.enable_garbage_collection;
    bops_.blob_file_size = config.blob_file_size;
    bops_.ttl_range_secs = config.blob_ttl_range;
    // compress
    auto compress_type = static_cast<rocksdb::CompressionType>(config.blob_compression);
    switch (compress_type) {
        case rocksdb::kSnappyCompression: // 1
        case rocksdb::kZlibCompression:  // 2
        case rocksdb::kBZip2Compression: // 3
        case rocksdb::kLZ4Compression: // 4
        case rocksdb::kLZ4HCCompression: // 5
        case rocksdb::kXpressCompression: // 6
        case rocksdb::kZSTD:
            bops_.compression = compress_type;
            break;
        default:
            bops_.compression = rocksdb::kNoCompression;
    }

#ifdef BLOB_EXTEND_OPTIONS
    bops_.gc_file_expired_percent = config.blob_gc_percent;
    if (config.blob_cache_size > 0) {
        bops_.blob_cache = rocksdb::NewLRUCache(config.blob_cache_size);
    }
#endif
}

Status RocksDBImpl::Open() {
    // 创建db的父目录
    if (MakeDirAll(db_path_, 0755) != 0) {
        return Status(Status::kIOError, "MakeDirAll", strErrno(errno));
    }

    ops_.create_if_missing = true;
    ops_.create_missing_column_families = true;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    // default column family
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    // txn column family
    column_families.emplace_back(kTxnCFName, rocksdb::ColumnFamilyOptions());

    std::vector<int32_t> ttls{ttl_, 0}; // keep same vector index with column_families
    rocksdb::Status ret;
    if (!is_blob_) {
        if (ttl_ == 0) {
            ret = rocksdb::DB::Open(ops_, db_path_, column_families, &cf_handles_, &db_);
        } else if (ttl_ > 0) { // with ttl
            rocksdb::DBWithTTL *ttl_db = nullptr;
            ret = rocksdb::DBWithTTL::Open(ops_, db_path_, column_families, &cf_handles_, &ttl_db, ttls);
            db_ = ttl_db;
        } else {
            return Status(Status::kInvalidArgument, "ttl", std::to_string(ttl_));
        }
    } else {
        if (ttl_ != 0) {
            return Status(Status::kNotSupported, "blob db ttl", std::to_string(ttl_));
        }
        rocksdb::blob_db::BlobDB *bdb = nullptr;
        ret = rocksdb::blob_db::BlobDB::Open(ops_, bops_, db_path_, column_families, &cf_handles_, &bdb);
        db_ = bdb;
    }

    // check open ret
    if (!ret.ok()) {
        return Status(Status::kIOError, "open db", ret.ToString());
    }

    // assign to context
    assert(cf_handles_.size() == 2);
    txn_cf_ = cf_handles_[1];
    return Status::OK();
}

RocksDBImpl::~RocksDBImpl() {
    for (auto handle: cf_handles_) {
        delete handle;
    }
    delete db_;
}

Status RocksDBImpl::Get(const std::string &key, std::string *value) {
    auto s = db_->Get(read_options_, key, value);
    if (s.ok()) {
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status(Status::kNotFound);
    } else {
        return Status(Status::kIOError, "Get", s.ToString());
    }
}

Status RocksDBImpl::Get(void* column_family,
           const std::string& key, std::string* value) {
    auto s = db_->Get(read_options_, static_cast<rocksdb::ColumnFamilyHandle*>(column_family),
                      key, value);
    if (s.ok()) {
        return Status::OK();
    } else if (s.IsNotFound()) {
        return Status(Status::kNotFound);
    } else {
        return Status(Status::kIOError, "Get", s.ToString());
    }
}

Status RocksDBImpl::Put(const std::string &key, const std::string &value) {
    auto s = db_->Put(write_options_, key, value);
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "PUT", s.ToString());
    }
}

Status RocksDBImpl::Put(void* column_family, const std::string& key, const std::string& value) {
    auto s = db_->Put(write_options_, static_cast<rocksdb::ColumnFamilyHandle*>(column_family), key, value);
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "PUT", s.ToString());
    }
}

std::unique_ptr<WriteBatchInterface> RocksDBImpl::NewBatch() {
    return std::unique_ptr<WriteBatchInterface>(new RocksWriteBatch);
}

Status RocksDBImpl::Write(WriteBatchInterface* batch) {
    auto s = db_->Write(write_options_, dynamic_cast<RocksWriteBatch*>(batch)->getBatch());
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "Write", s.ToString());
    }
}

Status RocksDBImpl::Delete(const std::string &key) {
    auto s = db_->Delete(write_options_, key);
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "Delete", s.ToString());
    }
}

Status RocksDBImpl::Delete(void* column_family, const std::string& key) {
    auto s = db_->Delete(write_options_, static_cast<rocksdb::ColumnFamilyHandle*>(column_family), key);
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "Delete", s.ToString());
    }
}

Status RocksDBImpl::DeleteRange(void *column_family,
                               const std::string &begin_key, const std::string &end_key) {
    auto s = db_->DeleteRange(write_options_,
                              static_cast<rocksdb::ColumnFamilyHandle*>(column_family),
                              begin_key, end_key);
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "DeleteRange", s.ToString());
    }
}

void* RocksDBImpl::DefaultColumnFamily() {
    return db_->DefaultColumnFamily();
}

void* RocksDBImpl::TxnCFHandle() {
    return txn_cf_;
}

IteratorInterface* RocksDBImpl::NewIterator(const std::string& start, const std::string& limit) {
    auto it = db_->NewIterator(read_options_);
    return new RocksIterator(it, start, limit);
}

Status RocksDBImpl::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                    std::unique_ptr<IteratorInterface>& txn_iter,
                    const std::string& start, const std::string& limit) {
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    cf_handles.push_back(db_->DefaultColumnFamily());
    cf_handles.push_back(txn_cf_);

    std::vector<rocksdb::Iterator*> iterators;
    rocksdb::ReadOptions rops;
    rops.fill_cache = false;
    auto s = db_->NewIterators(rops, cf_handles, &iterators);
    if (!s.ok()) {
        return Status(Status::kIOError, "create iterators", s.ToString());
    }

    assert(iterators.size() == 2);
    data_iter.reset(new RocksIterator(iterators[0], start, limit));
    txn_iter.reset(new RocksIterator(iterators[1], start, limit));

    return Status::OK();
}

void RocksDBImpl::GetProperty(const std::string& k, std::string* v) {
    db_->GetProperty(k, v);
}


Status RocksDBImpl::SetOptions(void* column_family,
                              const std::unordered_map<std::string, std::string>& new_options) {
    auto s = db_->SetOptions(static_cast<rocksdb::ColumnFamilyHandle*>(column_family), new_options);
    if (s.ok()) {
        return Status::OK();
    } else {
        return Status(Status::kIOError, "SetOptions", s.ToString());
    }
}

Status RocksDBImpl::SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) {
    auto s = db_->SetDBOptions(new_options);
    return Status(static_cast<Status::Code>(s.code()));
}

Status RocksDBImpl::CompactRange(const rocksdb::CompactRangeOptions& ops,
        rocksdb::Slice* start, rocksdb::Slice* end) {
    auto s = db_->CompactRange(ops, start, end);
    if (!s.ok()) {
        return Status(Status::kIOError, "CompactRange", s.ToString());
    } else {
        return Status::OK();
    }
}

Status RocksDBImpl::Flush(const rocksdb::FlushOptions& ops) {
    auto s = db_->Flush(ops);
    if (!s.ok()) {
        return Status(Status::kIOError, "Flush", s.ToString());
    } else {
        return Status::OK();
    }
}

std::string RocksDBImpl::GetMetrics(bool) {
    std::ostringstream ss;

    std::string tr_mem_usage;
    db_->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem_usage);
    std::string mem_table_usage;
    db_->GetProperty("rocksdb.cur-size-all-mem-tables", &mem_table_usage);

    ss << "rocksdb memory usages: table-readers=" << tr_mem_usage;
    ss << ", memtables=" << mem_table_usage;
    ss << ", block-cache=" << (block_cache_ ? block_cache_->GetUsage() : 0);
    ss << ", pinned=" << (block_cache_ ? block_cache_->GetPinnedUsage() : 0);
    ss << ", row-cache=" << (row_cache_ ? row_cache_->GetUsage() : 0);
    ss << std::endl;

    auto stat = db_stats_;
    if (stat) {
        ss << "rocksdb row-cache stats: hit="  << stat->getAndResetTickerCount(rocksdb::ROW_CACHE_HIT);
        ss << ", miss="  << stat->getAndResetTickerCount(rocksdb::ROW_CACHE_MISS);
        ss << std::endl;

        ss << "rocksdb block-cache stats: hit="  << stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_HIT);
        ss << ", miss="  << stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_MISS);
        ss << std::endl;

#ifdef BLOB_EXTEND_OPTIONS
        if (bops_.blob_cache) {
            ss << "rocksdb blobdb-cache stats: hit="  << stat->getAndResetTickerCount(rocksdb::BLOB_DB_CACHE_HIT);
            ss << ", miss="  << stat->getAndResetTickerCount(rocksdb::BLOB_DB_CACHE_MISS);
            ss << std::endl;
        }
#endif
        ss << "rockdb get histograms: " << stat->getHistogramString(rocksdb::DB_GET) << std::endl;
        ss << "rockdb write histograms: " <<  stat->getHistogramString(rocksdb::DB_WRITE) << std::endl;

        stat->Reset();
    }

    return ss.str();
}

}
}
}
