#include <gtest/gtest.h>

#include "base/util.h"
#include "storage/db/mass_tree_impl/mass_tree_db.h"
#include "storage/db/mass_tree_impl/mass_tree_mvcc.h"
#include "storage/db/mass_tree_impl/scaner.h"
#include "storage/db/multi_v_key.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace sharkstore;
using namespace sharkstore::dataserver::storage;

namespace {

TEST(MassTree, MVCCKey) {
    for (int i = 0; i < 100; ++i) {
        auto user_key = randomString(10, 20);
        auto ver = randomInt();
        MultiVersionKey key(user_key, ver, i % 2 == 0);
        auto s = key.to_string();
        MultiVersionKey key2;
        ASSERT_TRUE(key2.from_string(s));
        ASSERT_EQ(key2, key);
    }
}

TEST(MassTree, PutGet) {
    MassTreeDB db;
    for (int i = 0; i < 100; ++i) {
        std::string key = sharkstore::randomString(32);
        std::string value = sharkstore::randomString(64);
        db.Put(key, value);

        std::string actual_value;
        auto s = db.Get(key, &actual_value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(actual_value, value);
    }
}

TEST(MvccMassTree, PutGet) {
    MvccMassTree tree;
    for (int i = 0; i < 100; ++i) {
        std::string key = sharkstore::randomString(32);
        std::string value = sharkstore::randomString(64);
        tree.Put(key, value);

        std::string actual_value;
        auto s = tree.Get(key, &actual_value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(actual_value, value);
    }
}

TEST(MvccMassTree, Iter) {
    MvccMassTree tree;
    tree.Put("a", "b");
    tree.Put("aa", "b");
    tree.Put("b", "b");
    auto iter = tree.NewIterator(std::string("\0", 1), "\xff");
    while (iter->Valid()) {
        std::cout << "############ key: " << iter->key() <<  std::endl;
        iter->Next();
    }
}





} /* namespace  */
