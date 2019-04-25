#include <gtest/gtest.h>

#include "storage/db/mass_tree_impl/mass_tree_db.h"
#include "storage/db/mass_tree_impl/scaner.h"
#include "common/ds_encoding.h"
#include "base/util.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::storage;

class ScanerTest: public ::testing::Test {
public:
    ScanerTest() = default;
    ~ScanerTest() = default;

    void SetUp() override {
    }

    void TearDown() override {
    }

    MassTreeDB tree_;
};

void test_scan(std::unique_ptr<Scaner> scaner_ptr,
               uint64_t expected_starti, uint64_t* expected_endi) {
    auto i = expected_starti;
    auto scaner = scaner_ptr.get();
    while (scaner->Valid()) {
        auto k = scaner->Key();
        auto v = scaner->Value();

//        std::string buf;
//        EncodeUvarintAscending(&buf, i);
//        ASSERT_TRUE(k == buf);

        scaner->Next();
        ++i;
    }
    *expected_endi = i;
}

TEST_F(ScanerTest, rows100) {
    for (auto i = 0; i < 10; i++) {
        std::string buf;
        EncodeUvarintAscending(&buf, i);
        tree_.Put(buf, buf);
    }

    { // "" ... ""
        std::string start("");
        std::string end("");
        uint64_t expected_endi;

        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }

    { // "" ... 1
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 1);
        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 1);
    }
    { // "" ... 2
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 2);
        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 2);
    }
    { // "" ... 3
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 3);
        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 3);
    }

    { // "" ... 9
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 9);
        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 9);
    }
    { // "" ... 10
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 10);
        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }
    { // "" ... 11
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&end, 11);
        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }

    { // 1 ... 9
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&start, 1);
        EncodeUvarintAscending(&end, 9);
        test_scan(tree_.NewScaner(start, end), 1, &expected_endi);
        ASSERT_TRUE(expected_endi == 9);
    }
    { // 1 ... 10
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&start, 1);
        EncodeUvarintAscending(&end, 10);
        test_scan(tree_.NewScaner(start, end), 1, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }
    { // 1 ... 11
        std::string start;
        std::string end;
        uint64_t expected_endi;

        EncodeUvarintAscending(&start, 1);
        EncodeUvarintAscending(&end, 11);
        test_scan(tree_.NewScaner(start, end), 1, &expected_endi);
        ASSERT_TRUE(expected_endi == 10);
    }
}

TEST_F(ScanerTest, rows234) {
    for (auto i = 0; i < 234; i++) {
        std::string key;
        EncodeUvarintAscending(&key, i);
        tree_.Put(key, key);
    }
    for (auto i = 0; i < 234; i++) {
        std::string key;
        EncodeUvarintAscending(&key, i);

        std::string value;
        tree_.Get(key, &value);
        ASSERT_TRUE(value == key);
    }

    { // "" ... ""
        std::string start("");
        std::string end("");
        uint64_t expected_endi;

        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 234);
    }

}

TEST_F(ScanerTest, rows500) {
    for (auto i = 0; i < 500; i++) {
        auto key = sharkstore::randomString(200);
        tree_.Put(key, key);
    }
    { // "\0" ... "\xff"
        std::string start("");
        std::string end("\xff", 1);
        uint64_t expected_endi;

        test_scan(tree_.NewScaner(start, end), 0, &expected_endi);
        ASSERT_TRUE(expected_endi == 500);
    }
}

}

