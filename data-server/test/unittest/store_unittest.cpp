#include <gtest/gtest.h>

#include "base/status.h"
#include "base/util.h"
#include "storage/store.h"


#include "helper/query_builder.h"
#include "helper/query_parser.h"
#include "helper/helper_util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class StoreTest : public ::testing::Test {
protected:
    void SetUp() override {
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

        // create table;
        table_ = CreateAccountTable();

        // make meta
        meta_ = MakeRangeMeta(table_.get());

        store_ = new Store(meta_, db_);
    }

    void TearDown() override {
        delete store_;
        delete db_;
        if (!tmp_dir_.empty()) {
            DestroyDB(tmp_dir_, rocksdb::Options());
        }
    }

protected:
    std::string tmp_dir_;
    rocksdb::DB* db_ = nullptr;
    std::unique_ptr<Table> table_;
    metapb::Range meta_;
    Store* store_ = nullptr;
};

TEST_F(StoreTest, KeyValue) {
    // test put and get
    std::string key = sharkstore::randomString(32);
    std::string value = sharkstore::randomString(64);
    auto s = store_->Put(key, value);
    ASSERT_TRUE(s.ok());

    std::string actual_value;
    s = store_->Get(key, &actual_value);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(actual_value, value);

    // test delete and get
    s = store_->Delete(key);
    ASSERT_TRUE(s.ok());
    s = store_->Get(key, &actual_value);
    ASSERT_FALSE(s.ok());
    ASSERT_EQ(s.code(), sharkstore::Status::kNotFound);
}

TEST_F(StoreTest, SelectEmpty) {
    // all fields
    {
        SelectRequestBuilder builder(table_.get());
        builder.AddAllFields();
        auto req = builder.Build();
        kvrpcpb::SelectResponse resp;
        auto s = store_->Select(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(resp.code(), 0);
        ASSERT_EQ(resp.rows_size(), 0);
    }
    // random
    {
        for (int i = 0; i < 100; ++i) {
            SelectRequestBuilder builder(table_.get());
            builder.AddRandomFields();
            auto req = builder.Build();
            kvrpcpb::SelectResponse resp;
            auto s = store_->Select(req, &resp);
            ASSERT_TRUE(s.ok()) << s.ToString();
            ASSERT_EQ(resp.code(), 0);
            ASSERT_EQ(resp.rows_size(), 0);
        }
    }
    // select count
    {
        SelectRequestBuilder builder(table_.get());
        builder.AddAggreFunc("count", "");
        auto req = builder.Build();
        kvrpcpb::SelectResponse resp;
        auto s = store_->Select(req, &resp);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(resp.code(), 0);
        ASSERT_EQ(resp.rows_size(), 1);

        std::cout << resp.DebugString() << std::endl;

        SelectResultParser parser(req, resp);
        auto row = parser.GetRows();
        ASSERT_EQ(row.size(), 1);
        ASSERT_EQ(row[0].size(), 1);
        ASSERT_EQ(row[0][0], "0");




    }
}

TEST_F(StoreTest, SQL) {
}


} /* namespace  */
