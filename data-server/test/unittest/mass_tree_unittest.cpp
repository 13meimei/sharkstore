#include <gtest/gtest.h>

#include "base/util.h"
#include "storage/db/mass_tree_impl/mass_tree_impl.h"
#include "storage/db/mass_tree_impl/scaner.h"
#include "storage/db/multi_v_key.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

using namespace sharkstore;
using namespace sharkstore::dataserver::storage;

class MassTreeTest: public ::testing::Test {
protected:
    void SetUp() override {
        t_ = new MassTreeDBImpl();
    }

    void TearDown() override {
        delete t_;
    }

    std::unique_ptr<Scaner> Scan(const std::string& start, const std::string& end) {
        std::unique_ptr<Scaner> ptr(new Scaner(t_->default_tree_, start, end));
        return ptr;
    }

    MassTreeDBImpl* t_ = nullptr;
};

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

    std::string s("\022a\000\001\206\376\377", 7);
    MultiVersionKey key;
    ASSERT_TRUE(key.from_string(s));
    std::cout << "key: " << key.key() << ", ver: " << key.ver() << std::endl;
}

//TEST_F(MassTreeTest, Scan) {
//    t_->Put("a", "b");
//    auto iter = Scan(std::string("\0", 1), std::string("\xff"));
//    ASSERT_TRUE(iter->Valid());
//    while (iter->Valid()) {
//        MultiVersionKey key;
//        ASSERT_TRUE(key.from_string(iter->Key()));
//        std::cout << "key: " << key.key() << ", " << key.ver() << std::endl;
//        std::cout << "value: " << iter->Value() << std::endl;
//        iter->Next();
//    }
//}


TEST(MassTreeTest, PutGet) {
    auto t = new MassTreeDBImpl;
    t->Put("a", "b");
    std::string value;
    auto s = t->Get("a", &value);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(value, "b");
}






} /* namespace  */
