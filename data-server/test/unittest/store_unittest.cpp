#include <gtest/gtest.h>

#include "base/util.h"
#include "helper/store_test_fixture.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::test::helper;
using namespace sharkstore::dataserver;

// test base the account table
class StoreTest : public StoreTestFixture {
public:
    StoreTest() : StoreTestFixture(CreateAccountTable()) {}
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

TEST_F(StoreTest, EmptySelect) {
    // all fields
    auto s = testSelect(
            [](SelectRequestBuilder& b) { b.AddAllFields(); },
            {});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // random
    for (int i = 0; i < 100; ++i) {
        auto s = testSelect(
                [](SelectRequestBuilder& b) { b.AddRandomFields(); },
                {});
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count
    s = testSelect(
            [](SelectRequestBuilder& b) { b.AddAggreFunc("count", ""); },
            {{"0"}});
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StoreTest, Insert) {
    // one
    auto s = testInsert({{"1", "user1", "1.1"}});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // multi
    {
        std::vector<std::vector<std::string>> rows;
        for (int i = 0; i < 100; ++i) {
            rows.push_back({std::to_string(i), "user", "1.1"});
        }
        auto s = testInsert(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // insert check duplicate
    {
        InsertRequestBuilder builder(table_.get());
        builder.AddRow({"1", "user1", "1.1"});
        builder.SetCheckDuplicate();
        auto req = builder.Build();
        uint64_t affected = 0;
        auto s = store_->Insert(req, &affected);
        ASSERT_FALSE(s.ok());
        ASSERT_EQ(s.code(), sharkstore::Status::kDuplicate);
        ASSERT_EQ(affected, 0);
    }
}

TEST_F(StoreTest, MoreSelect) {
    // make test rows
    std::vector<std::vector<std::string>> rows;
    for (int i = 0; i < 100; ++i) {
        std::vector<std::string> row;
        row.push_back(std::to_string(i));
        row.push_back(std::string("user-") + std::to_string(i));
        row.push_back(std::to_string(i) + "." + std::to_string(5));
        rows.push_back(std::move(row));
    }

    // insert some data
    {
        auto s = testInsert(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // Select one row per loop
    {
        for (size_t i = 0; i < rows.size(); ++i) {
        }
    }
}

TEST_F(StoreTest, SQLDelete) {
}


} /* namespace  */
