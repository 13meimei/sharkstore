//
// Created by young on 19-2-14.
//

#include "store.h"
#include <rocksdb/utilities/blob_db/blob_db.h>
#include <common/ds_config.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/rate_limiter.h>
#include <base/util.h>
#include <rocksdb/utilities/db_ttl.h>
#include <frame/sf_logger.h>

namespace sharkstore {
namespace dataserver {
namespace storage {

static const std::string kDataPathSuffix = "data";
static const std::string kTxnCFName = "txn";

void RocksStore::buildDBOptions(rocksdb::Options& ops) {
	print_rocksdb_config();

	// db log level
	if (ds_config.rocksdb_config.enable_debug_log) {
		ops.info_log_level = rocksdb::DEBUG_LEVEL;
	}

	// db stats
	if (ds_config.rocksdb_config.enable_stats) {
		db_stats_ = rocksdb::CreateDBStatistics();
		ops.statistics = db_stats_;
	}

	// table options include block_size, block_cache_size, etc
	rocksdb::BlockBasedTableOptions table_options;
	table_options.block_size = ds_config.rocksdb_config.block_size;
	block_cache_ = rocksdb::NewLRUCache(ds_config.rocksdb_config.block_cache_size);
	if (ds_config.rocksdb_config.block_cache_size > 0) {
		table_options.block_cache = block_cache_;
	}
	if (ds_config.rocksdb_config.cache_index_and_filter_blocks){
		table_options.cache_index_and_filter_blocks = true;
	}
	ops.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

	// row_cache
	if (ds_config.rocksdb_config.row_cache_size > 0){
		row_cache_ = rocksdb::NewLRUCache(ds_config.rocksdb_config.row_cache_size);
		ops.row_cache = row_cache_;
	}

	ops.max_open_files = ds_config.rocksdb_config.max_open_files;
	ops.create_if_missing = true;
	ops.use_fsync = true;
	ops.use_adaptive_mutex = true;
	ops.bytes_per_sync = ds_config.rocksdb_config.bytes_per_sync;

	// memtables
	ops.write_buffer_size = ds_config.rocksdb_config.write_buffer_size;
	ops.max_write_buffer_number = ds_config.rocksdb_config.max_write_buffer_number;
	ops.min_write_buffer_number_to_merge =
			ds_config.rocksdb_config.min_write_buffer_number_to_merge;

	// level & sst file size
	ops.max_bytes_for_level_base = ds_config.rocksdb_config.max_bytes_for_level_base;
	ops.max_bytes_for_level_multiplier =
			ds_config.rocksdb_config.max_bytes_for_level_multiplier;
	ops.target_file_size_base = ds_config.rocksdb_config.target_file_size_base;
	ops.target_file_size_multiplier =
			ds_config.rocksdb_config.target_file_size_multiplier;

	// compactions and flushes
	if (ds_config.rocksdb_config.disable_auto_compactions) {
		FLOG_WARN("rocksdb auto compactions is disabled.");
		ops.disable_auto_compactions = true;
	}
	if (ds_config.rocksdb_config.background_rate_limit > 0) {
		ops.rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(
				rocksdb::NewGenericRateLimiter(static_cast<int64_t>(ds_config.rocksdb_config.background_rate_limit)));
	}
	ops.max_background_flushes = ds_config.rocksdb_config.max_background_flushes;
	ops.max_background_compactions = ds_config.rocksdb_config.max_background_compactions;
	ops.level0_file_num_compaction_trigger =
			ds_config.rocksdb_config.level0_file_num_compaction_trigger;

	// write pause
	ops.level0_slowdown_writes_trigger =
			ds_config.rocksdb_config.level0_slowdown_writes_trigger;
	ops.level0_stop_writes_trigger = ds_config.rocksdb_config.level0_stop_writes_trigger;

	// compress
	auto compress_type =
			static_cast<rocksdb::CompressionType>(ds_config.rocksdb_config.compression);
	switch (compress_type) {
		case rocksdb::kSnappyCompression: // 1
		case rocksdb::kZlibCompression:  // 2
		case rocksdb::kBZip2Compression: // 3
		case rocksdb::kLZ4Compression: // 4
		case rocksdb::kLZ4HCCompression: // 5
		case rocksdb::kXpressCompression: // 6
			ops.compression = compress_type;
			break;
		default:
			(void)ops.compression;
	}
}

void RocksStore::buildBlobOptions(rocksdb::blob_db::BlobDBOptions& bops) {
	assert(ds_config.rocksdb_config.min_blob_size >= 0);
	bops.min_blob_size = static_cast<uint64_t>(ds_config.rocksdb_config.min_blob_size);
	bops.enable_garbage_collection = ds_config.rocksdb_config.enable_garbage_collection;
	bops.blob_file_size = ds_config.rocksdb_config.blob_file_size;
	bops.ttl_range_secs = ds_config.rocksdb_config.blob_ttl_range;
	// compress
	auto compress_type =
			static_cast<rocksdb::CompressionType>(ds_config.rocksdb_config.blob_compression);
	switch (compress_type) {
		case rocksdb::kSnappyCompression: // 1
		case rocksdb::kZlibCompression:  // 2
		case rocksdb::kBZip2Compression: // 3
		case rocksdb::kLZ4Compression: // 4
		case rocksdb::kLZ4HCCompression: // 5
		case rocksdb::kXpressCompression: // 6
		case rocksdb::kZSTD:
			bops.compression = compress_type;
			break;
		default:
			(void)bops.compression;
	}

#ifdef BLOB_EXTEND_OPTIONS
	bops.gc_file_expired_percent = ds_config.rocksdb_config.blob_gc_percent;
    if (ds_config.rocksdb_config.blob_cache_size > 0) {
        bops.blob_cache = rocksdb::NewLRUCache(ds_config.rocksdb_config.blob_cache_size);
    }
#endif
}

int RocksStore::openDB() {
    auto write_options = rocksdb::WriteOptions();
    write_options.disableWAL = ds_config.rocksdb_config.disable_wal;
    auto read_options = rocksdb::ReadOptions(ds_config.rocksdb_config.read_checksum,true);

    // 创建db的父目录
    auto db_path = JoinFilePath({ds_config.rocksdb_config.path, kDataPathSuffix});
    if (MakeDirAll(db_path, 0755) != 0) {
        FLOG_ERROR("create rocksdb directory(%s) failed(%s)", db_path.c_str(), strErrno(errno).c_str());
        return -1;
    }

    rocksdb::Options ops;
    buildDBOptions(ops);
    ops.create_missing_column_families = true;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    // default column family
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    // txn column family
    column_families.emplace_back(kTxnCFName, rocksdb::ColumnFamilyOptions());
    auto ttl = ds_config.rocksdb_config.ttl;
    std::vector<int32_t> ttls{ttl, 0}; // keep same vector index with column_families

    rocksdb::Status ret;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    if (ds_config.rocksdb_config.storage_type == 0){ // open normal rocksdb
        if (ttl == 0) {
            ret = rocksdb::DB::Open(ops, db_path, column_families, &cf_handles, &db_);
        } else if (ttl > 0) { // with ttl
            FLOG_WARN("rocksdb ttl enabled. ttl=%d", ttl);
            rocksdb::DBWithTTL *ttl_db = nullptr;
            ret = rocksdb::DBWithTTL::Open(ops, db_path, column_families, &cf_handles, &ttl_db, ttls);
            db_ = ttl_db;
        } else {
            FLOG_ERROR("invalid rocksdb ttl: %d", ttl);
            return -1;
        }
    } else if (ds_config.rocksdb_config.storage_type == 1) { // open blobdb
        rocksdb::blob_db::BlobDBOptions bops;
        buildBlobOptions(bops);
        rocksdb::blob_db::BlobDB *bdb = nullptr;
        ret = rocksdb::blob_db::BlobDB::Open(ops, bops, db_path, column_families, &cf_handles, &bdb);
        db_ = bdb;
    } else {
        FLOG_ERROR("invalid rocksdb storage_type(%d)", ds_config.rocksdb_config.storage_type);
        return -1;
    }

    // check open ret
    if (!ret.ok()) {
        FLOG_ERROR("open rocksdb failed(%s): %s", db_path.c_str(), ret.ToString().c_str());
        return -1;
    }

    // assign to context
    assert(cf_handles.size() == 2);
    txn_cf_ = cf_handles[1];
    return 0;
}

RocksStore::RocksStore() {
    openDB();
}

RocksStore::~RocksStore() {
    delete db_;
    delete txn_cf_;
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

Status RocksStore::Delete(void* column_family, const std::string& key) {
    auto s = db_->Delete(write_options_, static_cast<rocksdb::ColumnFamilyHandle*>(column_family), key);
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

void* RocksStore::TxnCFHandle() {
    return txn_cf_;
}

IteratorInterface* RocksStore::NewIterator(const std::string& start, const std::string& limit) {
    auto it = db_->NewIterator(read_options_);
    return new RocksIterator(it, start, limit);
}

Status RocksStore::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
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

void RocksStore::PrintMetric() {
    std::string tr_mem_usage;
    db_->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem_usage);
    std::string mem_table_usage;
    db_->GetProperty("rocksdb.cur-size-all-mem-tables", &mem_table_usage);
    FLOG_INFO("rocksdb memory usages: table-readers=%s, memtables=%s, "
              "block-cache=%lu, pinned=%lu, row-cache=%lu",
              tr_mem_usage.c_str(), mem_table_usage.c_str(),
              block_cache_->GetUsage(),
              block_cache_->GetPinnedUsage(),
              (row_cache_ ? row_cache_->GetUsage() : 0));

    auto stat = db_stats_;
    if (stat) {
        FLOG_INFO("rocksdb row-cache stats: hit=%" PRIu64 ", miss=%" PRIu64,
                  stat->getAndResetTickerCount(rocksdb::ROW_CACHE_HIT),
                  stat->getAndResetTickerCount(rocksdb::ROW_CACHE_MISS));

        FLOG_INFO("rocksdb block-cache stats: hit=%" PRIu64 ", miss=%" PRIu64,
                  stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_HIT),
                  stat->getAndResetTickerCount(rocksdb::BLOCK_CACHE_MISS));

#ifdef BLOB_EXTEND_OPTIONS
        if (ds_config.rocksdb_config.blob_cache_size > 0) {
            FLOG_INFO("rocksdb blobdb-cache stats: hit=%" PRIu64 ", miss=%" PRIu64,
                      stat->getAndResetTickerCount(rocksdb::BLOB_DB_CACHE_HIT),
                      stat->getAndResetTickerCount(rocksdb::BLOB_DB_CACHE_MISS));
        }
#endif

        FLOG_INFO("rockdb get histograms: %s", stat->getHistogramString(rocksdb::DB_GET).c_str());
        FLOG_INFO("rockdb write histograms: %s", stat->getHistogramString(rocksdb::DB_WRITE).c_str());

        stat->Reset();
    }
}

}}}
