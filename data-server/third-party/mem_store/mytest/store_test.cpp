#include <gtest/gtest.h>

#include "mem_store/mem_store.h"
#include "sl_map.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

class MemStoreTest: public ::testing::Test {
protected:
    void SetUp() override {
        store_0_.Put(std::string("a"), std::string("aa"));
        store_0_.Put(std::string("b"), std::string("bb"));
        store_0_.Put(std::string("c"), std::string("cc"));
    }

    memstore::Store<std::string> store_0_;

    void TearDown() override {}
};

TEST_F(MemStoreTest, Get) {
    auto v = std::string();
    auto ret = -1;

    ret = store_0_.Get("a", &v);
    ASSERT_TRUE(ret == 0 && v == "aa");
    ret = store_0_.Get("b", &v);
    ASSERT_TRUE(ret == 0 && v == "bb");
    ret = store_0_.Get("c", &v);
    ASSERT_TRUE(ret == 0 && v == "cc");
}

TEST_F(MemStoreTest, Del) {
    auto k = std::string("a");
    auto v = std::string();
    auto ret = -1;

    // get a
    ret = store_0_.Get(k, &v);
    ASSERT_TRUE(ret == 0 && v == "aa");

    // del a
    auto n_del = store_0_.Delete(k);
    std::cout << "n_del: " << n_del << std::endl;
//    ASSERT_TRUE(n_del == 1);

    // get a
    ret = store_0_.Get(k, &v);
    ASSERT_TRUE(ret == -1);
}

TEST_F(MemStoreTest, DelRange) {
    auto v = std::string();
    auto ret = -1;

    ret = store_0_.Get("a", &v);
    ASSERT_TRUE(ret == 0 && v == "aa");
    ret = store_0_.Get("b", &v);
    ASSERT_TRUE(ret == 0 && v == "bb");
    ret = store_0_.Get("c", &v);
    ASSERT_TRUE(ret == 0 && v == "cc");

    // del a b
    auto n_del_range = store_0_.DeleteRange("a", "c");
    std::cout << "n_del_range: " << n_del_range << std::endl;
    ASSERT_TRUE(n_del_range == 2);

    ret = store_0_.Get("a", &v);
    ASSERT_TRUE(ret == -1);
    ret = store_0_.Get("b", &v);
    ASSERT_TRUE(ret == -1);
    ret = store_0_.Get("c", &v);
    ASSERT_TRUE(ret == 0 && v == "cc");
}

TEST_F(MemStoreTest, Iter) {
    auto it = store_0_.NewIterator("a", "d");
    ASSERT_TRUE(it != nullptr);

    // it a
    std::cout << it->Key() << std::endl;
    ASSERT_TRUE(it->Key() == "a" && it->Value() == "aa");

    it->Next();
    std::cout << it->Key() << std::endl;
    ASSERT_TRUE(it->Key() == "b" && it->Value() == "bb");

    it->Next();
    std::cout << it->Key() << std::endl;
    ASSERT_TRUE(it->Key() == "c" && it->Value() == "cc");
}

TEST_F(MemStoreTest, IterValid) {
    auto it = store_0_.NewIterator("a", "d");
    ASSERT_TRUE(it != nullptr);

    auto loop_end = false;
    for (auto i = 0; i < 4; i++, it->Next()) {
        if (!it->Valid()) {
            break;
        }
        switch (i) {
            case 0:
                std::cout << it->Key() << std::endl;
                ASSERT_TRUE(it->Key() == "a" && it->Value() == "aa");
                break;
            case 1:
                std::cout << it->Key() << std::endl;
                ASSERT_TRUE(it->Key() == "b" && it->Value() == "bb");
                break;
            case 2:
                std::cout << it->Key() << std::endl;
                ASSERT_TRUE(it->Key() == "c" && it->Value() == "cc");
                loop_end = true;
                break;
            default:
                std::cout << "loop times != 3" << std::endl;
                ASSERT_TRUE(false);
        }
    }

    ASSERT_TRUE(loop_end);
}

TEST_F(MemStoreTest, IterLoop) {
    auto key = std::string("\001\000\000\000\000\000\000\000\003\211", 10);
    auto value = std::string("&\007myname1");

    store_0_.Put(key, value);
    std::string v;
    store_0_.Get(key, &v);
    std::cout << "v: " << v << std::endl;
    ASSERT_TRUE(v == value);

    auto scope_start = std::string("\001\000\000\000\000\000\000\000\003", 9);
    auto scope_end = std::string("\001\000\000\000\000\000\000\000\004", 9);

    auto it = store_0_.NewIterator(scope_start, scope_end);
    ASSERT_TRUE(it->Valid());
    for (; it->Valid(); it->Next()) {
        std::cout << "k: " << it->Key() << " v: " << it->Value() << std::endl;
    }
}

TEST_F(MemStoreTest, Seek) {
    auto it = store_0_.Seek("aa");
    std::cout << "seek: " << it->first << std::endl;
    ASSERT_TRUE(it->first== "b");
}

TEST_F(MemStoreTest, InsertDup) {
    store_0_.Put("a", "1");

    std::string v;
    auto ret = store_0_.Get("a", &v);
    ASSERT_TRUE(ret == 0);
    ASSERT_TRUE(v == "1");

}

} // namespace end
