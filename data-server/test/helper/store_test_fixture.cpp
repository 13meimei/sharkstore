#include "store_test_fixture.h"

#include "base/util.h"
#include "query_parser.h"
#include "helper_util.h"

#include "frame/sf_config.h"

#include "fastcommon/logger.h"
#include <fastcommon/shared_func.h>
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/skiplist_impl/skiplist_impl.h"
#include "storage/db/mass_tree_impl/mass_tree_mvcc.h"

namespace sharkstore {
namespace test {
namespace helper {

using namespace ::sharkstore::dataserver::range;
using namespace ::sharkstore::dataserver::storage;

StoreTestFixture::StoreTestFixture(std::unique_ptr<Table> t) :
    table_(std::move(t)) {
}

void StoreTestFixture::SetUp() {
    log_init2();
    char level[] = "info";
    set_log_level(level);

    if (!table_) {
        throw std::runtime_error("invalid table");
    }

    // open rocksdb
    char path[] = "/tmp/sharkstore_ds_store_test_XXXXXX";
    char* tmp = mkdtemp(path);
    ASSERT_TRUE(tmp != NULL);
    tmp_dir_ = tmp;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
    column_families.emplace_back("txn", rocksdb::ColumnFamilyOptions());

    rocksdb::Options ops;
    ops.create_if_missing = true;
    ops.error_if_exists = true;
//    db_ = new dataserver::storage::RocksDBImpl(ops, tmp_dir_);
    db_ = new dataserver::storage::MvccMassTree();
    auto s = db_->Open();
    ASSERT_TRUE(s.ok()) << s.ToString();

    // make meta
    meta_ = MakeRangeMeta(table_.get());

    store_ = new sharkstore::dataserver::storage::Store(meta_, db_);
}

void StoreTestFixture::TearDown() {
    delete store_;
    delete db_;
    if (!tmp_dir_.empty()) {
        RemoveDirAll(tmp_dir_.c_str());
    }
}

Status StoreTestFixture::testSelect(
        const std::function<void(SelectRequestBuilder&)>& build_func,
        const std::vector<std::vector<std::string>>& expected_rows) {
    SelectRequestBuilder builder(table_.get());
    build_func(builder);
    auto req = builder.Build();

    kvrpcpb::SelectResponse resp;
    auto s = store_->Select(req, &resp);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select", s.ToString());
    }
    if (resp.code() != 0) {
        return Status(Status::kUnexpected, "select code", std::to_string(resp.code()));
    }

    SelectResultParser parser(req, resp);
    s = parser.Match(expected_rows);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select rows", s.ToString());
    }
    return Status::OK();
}

Status StoreTestFixture::testTxnSelect(
        const std::function<void(TxnSelectRequestBuilder&)>& build_func,
        const std::vector<std::vector<std::string>>& expected_rows,
        std::vector<uint64_t>& versions) {
    TxnSelectRequestBuilder builder(table_.get());
    build_func(builder);
    auto req = builder.Build();

    txnpb::SelectResponse resp;
    auto s = store_->TxnSelect(req, &resp);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select", s.ToString());
    }
    if (resp.code() != 0) {
        return Status(Status::kUnexpected, "select code", std::to_string(resp.code()));
    }

    for (const auto& r: resp.rows()) {
        versions.emplace_back(r.value().version());
    }

    SelectResultParser parser(req, resp);
    s = parser.Match(expected_rows);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "select rows", s.ToString());
    }

    return Status::OK();
}

Status StoreTestFixture::testInsert(const std::vector<std::vector<std::string>> &rows, uint64_t *insert_bytes) {
    InsertRequestBuilder builder(table_.get());
    builder.AddRows(rows);
    auto req = builder.Build();

    uint64_t affected = 0;
    auto s = store_->Insert(req, &affected);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "insert", s.ToString());
    }
    if (affected != rows.size()) {
        return Status(Status::kUnexpected, "insert affected",
                std::string("expected: ") + std::to_string(rows.size()) +
                ", actual: " + std::to_string(affected));
    }
    if (insert_bytes != nullptr) {
        *insert_bytes = 0;
        for (const auto& row: req.rows()) {
            *insert_bytes += row.key().size();
            *insert_bytes += row.value().size();
        }
    }
    return Status::OK();
}

Status StoreTestFixture::testDelete(const std::function<void(DeleteRequestBuilder&)>& build_func,
                  uint64_t expected_affected) {
    DeleteRequestBuilder builder(table_.get());
    build_func(builder);
    auto req = builder.Build();

    uint64_t actual_affected = 0;
    auto s = store_->DeleteRows(req, &actual_affected);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "delete", s.ToString());
    }

    if (actual_affected != expected_affected) {
        return Status(Status::kUnexpected, "delete affected",
                      std::string("expected: ") + std::to_string(expected_affected) +
                      ", actual: " + std::to_string(actual_affected));
    }
    return Status::OK();
}

Status StoreTestFixture::testUpdate(const std::function<void(UpdateRequestBuilder&)>& build_func,
                  uint64_t expected_affected) {
    UpdateRequestBuilder builder(table_.get());
    build_func(builder);
    auto req = builder.Build();

    uint64_t actual_affected = 0;
    uint64_t update_bytes = 0;
    auto s = store_->Update(req, &actual_affected, &update_bytes);
    if (!s.ok()) {
        return Status(Status::kUnexpected, "update", s.ToString());
    }

    if (actual_affected != expected_affected) {
        return Status(Status::kUnexpected, "update affected",
                      std::string("expected: ") + std::to_string(expected_affected) +
                      ", actual: " + std::to_string(actual_affected));
    }
    return Status::OK();
}

std::string StoreTestFixture::encodeWatchKey(const std::vector<std::string>& keys) {
    watchpb::WatchKeyValue kv;
    for (const auto& key: keys) {
        kv.add_key(key);
    }
    return store_->encodeWatchKey(kv);
}

Status StoreTestFixture::testParseWatchSplitKey(const std::vector<std::string>& keys) {
    assert(!keys.empty());

    watchpb::WatchKeyValue kv;
    for (const auto& key: keys) {
        kv.add_key(key);
    }
    std::string enc_key = store_->encodeWatchKey(kv);
    std::string split_key;
    auto s = store_->parseSplitKey(enc_key, SplitKeyMode::kLockWatch, &split_key);
    if (!s.ok()) {
        return s;
    }

    watchpb::WatchKeyValue expected_kv;
    expected_kv.add_key(keys[0]);
    std::string expected_split_key = store_->encodeWatchKey(expected_kv);
    if (split_key != expected_split_key) {
        return Status(Status::kUnexpected, EncodeToHex(split_key), EncodeToHex(expected_split_key));
    }

    return Status::OK();
}

uint64_t StoreTestFixture::statSizeUntil(const std::string& end) {
    uint64_t size = 0;
    std::unique_ptr<dataserver::storage::IteratorInterface> it(store_->NewIterator("", end));
    while (it->Valid()) {
        size += it->key_size();
        size += it->value_size();
        it->Next();
    }
    return size;
}

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
