#include "store_test_fixture.h"

#include "query_parser.h"
#include "helper_util.h"

namespace sharkstore {
namespace test {
namespace helper {

StoreTestFixture::StoreTestFixture(std::unique_ptr<Table> t) :
    table_(std::move(t)) {
}

void StoreTestFixture::SetUp() {
    if (!table_) {
        throw std::runtime_error("invalid table");
    }

    // open rocksdb
    char path[] = "/tmp/sharkstore_ds_store_test_XXXXXX";
    char* tmp = mkdtemp(path);
    ASSERT_TRUE(tmp != NULL);
    tmp_dir_ = tmp;

    rocksdb::Options ops;
    ops.create_if_missing = true;
    ops.error_if_exists = true;
    auto s = rocksdb::DB::Open(ops, tmp, &db_);
    ASSERT_TRUE(s.ok());

    // make meta
    meta_ = MakeRangeMeta(table_.get());

    store_ = new sharkstore::dataserver::storage::Store(meta_, db_);
}

void StoreTestFixture::TearDown() {
    delete store_;
    delete db_;
    if (!tmp_dir_.empty()) {
        DestroyDB(tmp_dir_, rocksdb::Options());
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

Status StoreTestFixture::testInsert(const std::vector<std::vector<std::string>> &rows) {
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

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
