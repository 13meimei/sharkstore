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
    for (int i = 0; i < 1000; ++i) {
        std::string key = sharkstore::randomString(32, 1024);
        std::string value = sharkstore::randomString(64);
        db.Put(key, value);

        std::string actual_value;
        auto s = db.Get(key, &actual_value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(actual_value, value);
    }
}

TEST(MassTree, Iter) {
    struct KeyValue {
        std::string key;
        std::string value;

        bool operator<(const KeyValue &rh) const {
            return key < rh.key;
        }
    };

    std::set<KeyValue> key_values;
    MassTreeDB tree;
    for (int i = 0; i < 500; ++i) {
        std::string key = sharkstore::randomString(32, 1024);
        std::string value = sharkstore::randomString(64);
        tree.Put(key, value);
        key_values.insert(KeyValue{key, value});
    }
    auto tree_iter = tree.NewScaner("", "");
    int count = 0;
    auto set_iter = key_values.begin();
    while (tree_iter->Valid()) {
//        std::cout << tree_iter->Key() << std::endl;
        ASSERT_LT(count, key_values.size());
        ASSERT_EQ(tree_iter->Key(), set_iter->key) << " at index " << count;
        ASSERT_EQ(tree_iter->Value(), set_iter->value) << " at index " << count;
        ++count;
        ++set_iter;
        tree_iter->Next();
    }
    ASSERT_EQ(count, key_values.size());
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
    tree.Put("a", "value");
    tree.Put("aa", "value");
    tree.Put("bb", "value");
    tree.Delete("aa");
    tree.Delete("bb");
    auto iter = tree.NewIterator("", "");
    while (iter->Valid()) {
        std::cout << iter->key() << std::endl;
        iter->Next();
    }
}

TEST(MvccMassTree, DISABLED_GC) {
    MvccMassTree tree;
    tree.Open();
    std::set<std::string> keys;
    for (int i = 0; i < 100000; ++i) {
        auto key = randomString(10, 1024);
        auto s = tree.Put(key, "");
        ASSERT_TRUE(s.ok()) << s.ToString();
        keys.insert(key);
    }
    std::cout << "Put over. " << std::endl;
    std::cout << "======================================================= " << std::endl;
    std::cout << tree.GetMetrics(true) << std::endl;
    std::cout << "======================================================= " << std::endl;

    for (auto it = keys.cbegin(); it != keys.cend(); ++it) {
        std::string value;
        auto s = tree.Get(*it, &value);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_TRUE(value.empty());
    }

    for (auto it = keys.cbegin(); it != keys.cend(); ++it) {
        auto s = tree.Delete(*it);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }
    std::cout << "Delete over. " << std::endl;
    std::cout << tree.GetMetrics(true) << std::endl;

    for (auto it = keys.cbegin(); it != keys.cend(); ++it) {
        std::string value;
        auto s = tree.Get(*it, &value);
        ASSERT_EQ(s.code(), Status::kNotFound);
    }
    std::cout << "Get over. " << std::endl;
    std::cout << tree.GetMetrics(true) << std::endl;

    for (int i = 0; i < 100; ++i) {
        sleep(10);
        std::cout << tree.GetMetrics(true) << std::endl;
    }
}

} /* namespace  */
