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
        builder.AddRow({"1", "user1", "100"});
        builder.SetCheckDuplicate();
        auto req = builder.Build();
        uint64_t affected = 0;
        auto s = store_->Insert(req, &affected);
        ASSERT_FALSE(s.ok());
        ASSERT_EQ(s.code(), sharkstore::Status::kDuplicate);
        ASSERT_EQ(affected, 0);
    }
}

TEST_F(StoreTest, SelectEmpty) {
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

TEST_F(StoreTest, SelectFields) {
    // make test rows
    std::vector<std::vector<std::string>> rows;
    for (int i = 1; i <= 100; ++i) {
        std::vector<std::string> row;
        row.push_back(std::to_string(i));
        row.push_back(std::string("user-") + std::to_string(i));
        row.push_back(std::to_string(100 + i));
        rows.push_back(std::move(row));
    }
    // insert some data
    {
        auto s = testInsert(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select all rows
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                },
                rows
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // Select one row per loop, all fields
    {
        for (size_t i = 0; i < rows.size(); ++i) {
            auto s = testSelect(
                    [&rows, i](SelectRequestBuilder& b) {
                        b.AddAllFields();
                        b.SetKey({rows[i][0]});
                    },
                    {rows[i]}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }
    }

    // one filed
    {
        for (size_t i = 0; i < rows.size(); ++i) {
            auto s = testSelect(
                    [&rows, i](SelectRequestBuilder& b) {
                        b.AddField("id");
                        b.SetKey({rows[i][0]});
                    },
                    {{rows[i][0]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }

        for (size_t i = 0; i < rows.size(); ++i) {
            auto s = testSelect(
                    [&rows, i](SelectRequestBuilder& b) {
                        b.AddField("name");
                        b.SetKey({rows[i][0]});
                    },
                    {{rows[i][1]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }

        for (size_t i = 0; i < rows.size(); ++i) {
            auto s = testSelect(
                    [&rows, i](SelectRequestBuilder& b) {
                        b.AddField("balance");
                        b.SetKey({rows[i][0]});
                    },
                    {{rows[i][2]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }
    }

    // no pk fileds
    {
        for (size_t i = 0; i < rows.size(); ++i) {
            auto s = testSelect(
                    [&rows, i](SelectRequestBuilder& b) {
                        b.AddField("name");
                        b.AddField("balance");
                        b.SetKey({rows[i][0]});
                    },
                    {{rows[i][1], rows[i][2]}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }
    }
}

TEST_F(StoreTest, SelectWhere) {
    std::vector<std::vector<std::string>> rows;
    for (int i = 1; i <= 100; ++i) {
        std::vector<std::string> row;
        row.push_back(std::to_string(i));

        char name[32] = {'\0'};
        snprintf(name, 32, "user-%04d", i);
        row.push_back(name);

        row.push_back(std::to_string(100 + i));
        rows.push_back(std::move(row));
    }

    // insert some data
    {
        auto s = testInsert(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select * where id
    {
        // id == 1
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::Equal, "1");
                },
                {rows[0]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id != 1
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::NotEqual, "1");
                },
                {rows.cbegin() + 1, rows.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id < 3
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::Less, "3");
                },
                {rows[0], rows[1]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id <= 2
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::LessOrEqual, "2");
                },
                {rows[0], rows[1]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id > 2
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::Larger, "2");
                },
                {rows.cbegin() + 2, rows.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id >= 2
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::LargerOrEqual, "2");
                },
                {rows.cbegin() + 1, rows.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id > 1 and id < 4
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::Larger, "1");
                    b.AddMatch("id", kvrpcpb::Less, "4");
                },
                {rows[1], rows[2]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // id > 4 and id < 1
        s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("id", kvrpcpb::Larger, "4");
                    b.AddMatch("id", kvrpcpb::Less, "1");
                },
                {}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select where name
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("name", kvrpcpb::Larger, "user-0002");
                },
                {rows.cbegin() + 2, rows.cend()}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    // select where balance
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAllFields();
                    b.AddMatch("balance", kvrpcpb::Less, "103");
                },
                {rows[0], rows[1]}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(StoreTest, SelectAggreCount) {
    // make test rows
    std::vector<std::vector<std::string>> rows;
    for (int i = 1; i <= 100; ++i) {
        std::vector<std::string> row;
        row.push_back(std::to_string(i));
        row.push_back(std::string("user-") + std::to_string(i));
        row.push_back(std::to_string(100 + i));
        rows.push_back(std::move(row));
    }

    // insert some data
    {
        auto s = testInsert(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count(*)
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAggreFunc("count", "");
                },
                {{std::to_string(rows.size())}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count(*) where id = {id}
    {
        for (size_t i = 0; i < rows.size(); ++i) {
            auto s = testSelect(
                    [&rows, i](SelectRequestBuilder& b) {
                        b.SetKey({rows[i][0]});
                        b.AddAggreFunc("count", "");
                    },
                    {{"1"}}
            );
            ASSERT_TRUE(s.ok()) << s.ToString();
        }

        // not exist
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.SetKey({std::to_string(std::numeric_limits<int64_t>::max())});
                    b.AddAggreFunc("count", "");
                },
                {{"0"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select count(*) where id
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAggreFunc("count", "");
                    b.AddMatch("id", kvrpcpb::Equal, "1");
                },
                {{"1"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAggreFunc("count", "");
                    b.AddMatch("id", kvrpcpb::NotEqual, "1");
                },
                {{std::to_string(rows.size() - 1)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAggreFunc("count", "");
                    b.AddMatch("id", kvrpcpb::Less, "5");
                },
                {{"4"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    {
        auto s = testSelect(
                [](SelectRequestBuilder& b) {
                    b.AddAggreFunc("count", "");
                    b.AddMatch("id", kvrpcpb::Larger, "5");
                },
                {{std::to_string(rows.size() - 5)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(StoreTest, SelectAggreMore) {
    // make test rows
    std::vector<std::vector<std::string>> rows;
    for (int i = 1; i <= 100; ++i) {
        std::vector<std::string> row;
        row.push_back(std::to_string(i));
        row.push_back(std::string("user-") + std::to_string(i));
        row.push_back(std::to_string(100 + i));
        rows.push_back(std::move(row));
    }

    // insert some data
    {
        auto s = testInsert(rows);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // select aggre id
    {
        // max
        auto s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.AddAggreFunc("max", "id");
                },
                {{"100"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // min
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.AddAggreFunc("min", "id");
                },
                {{"1"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        int sum = 0;
        for (int i = 1; i <= 100; ++i) {
            sum += i;
        }
        // sum
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.AddAggreFunc("sum", "id");
                },
                {{std::to_string(sum)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // banlance
    {
        // max
        auto s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.AddAggreFunc("max", "balance");
                },
                {{"200"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        // min
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.AddAggreFunc("min", "balance");
                },
                {{"101"}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();

        int sum = 0;
        for (int i = 1; i <= 100; ++i) {
            sum += i;
            sum += 100;
        }
        // sum
        s = testSelect(
                [](SelectRequestBuilder &b) {
                    b.AddAggreFunc("sum", "balance");
                },
                {{std::to_string(sum)}}
        );
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
}

TEST_F(StoreTest, SQLDelete) {
}


} /* namespace  */
