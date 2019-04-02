#include <iostream>
#include <map>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include "storage/db/multi_v_key.h"
#include "helper/mock/mass_tree_mvcc_mock.h"
#include "helper/mock/mass_tree_iterator_mock.h"

using namespace sharkstore::test::mock;
using namespace sharkstore::dataserver::storage;

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {
class MassMvccTest : public testing::Test {
protected:
    MassMvccTest() = default;

    void SetUp() override {
        setenv("DB", "mass-tree-mock", 1);
        db_ = new MvccMassTreeMock();
    }
    void TearDown() override {

    }

protected:
    MvccMassTreeMock* db_;
};

TEST_F(MassMvccTest, SingleKey) {
    auto db = getenv("DB");
    ASSERT_STREQ(db, "mass-tree-mock");

    MultiVersionKey mulitKey;
    std::string k = "a";
    std::string v = "a";
    db_->Put(k, v);//ver=1

    std::unique_ptr<MassTreeIteratorMock> it(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=2
    ASSERT_TRUE(it->Valid());
    mulitKey = it->getMultiKey();
    //std::cout << mulitKey.key() << " ver=" << mulitKey.ver() << "::::" <<it->key() << std::endl;
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_STREQ(it->value().c_str(), "a");
    ASSERT_EQ(mulitKey.ver(), 1);

    db_->Delete(k);//ver=3

    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=4
    //mulitKey = it->getMultiKey();
    //std::cout << mulitKey.key() << " ver=" << mulitKey.ver() << "::::" <<it->key() << std::endl;
    ASSERT_FALSE(it->Valid());

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=5
    ASSERT_TRUE(it->Valid());

    mulitKey = it->getMultiKey();
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_TRUE(mulitKey.is_del());
    ASSERT_EQ(mulitKey.ver(), 3);

    it->Traverse();
    ASSERT_TRUE(it->Valid());
    mulitKey = it->getMultiKey();
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_STREQ(it->value().c_str(), "a");
    ASSERT_FALSE(mulitKey.is_del());
    ASSERT_EQ(mulitKey.ver(), 1);
}


TEST_F(MassMvccTest, MultiKey) {
    auto db = getenv("DB");
    ASSERT_STREQ(db, "mass-tree-mock");

    std::vector<std::pair<std::string, std::string>> kv;
    MultiVersionKey mulitKey;
    kv.emplace_back(std::make_pair("a", "va"));
    kv.emplace_back(std::make_pair("a", "va"));
    kv.emplace_back(std::make_pair("a", "va"));
    kv.emplace_back(std::make_pair("b", "vb"));
    kv.emplace_back(std::make_pair("b", "vb"));
    kv.emplace_back(std::make_pair("c", "vc"));
    kv.emplace_back(std::make_pair("d", "vd"));

    int i = 0;
    db_->Put(kv.at(i).first, kv.at(i).second);//ver=1
    i++;db_->Put(kv.at(i).first, kv.at(i).second);//ver=2
    i++;db_->Put(kv.at(i).first, kv.at(i).second);//ver=3
    i++;db_->Put(kv.at(i).first, kv.at(i).second);//ver=4
    i++;db_->Put(kv.at(i).first, kv.at(i).second);//ver=5

    i = 0;
    std::map<int, int> index_map;
    index_map[i++] = 2;//kv[2],a
    index_map[i++] = 4;//kv[3],b
    i = 0;
    std::map<int, uint64_t> ver_map;
    ver_map[i++] = 3;
    ver_map[i++] = 5;
    std::unique_ptr<MassTreeIteratorMock> it(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=6
    for (int i = 0; i < 2 && it->Valid(); i++) {
        mulitKey = it->getMultiKey();
        //std::cout << "i = " << i << " key = " << mulitKey.key() <<" ver=" << mulitKey.ver() << std::endl;
        ASSERT_STREQ(mulitKey.key().c_str(), kv.at(index_map[i]).first.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i]).second.c_str());
        ASSERT_EQ(mulitKey.ver(), ver_map[i]);
        it->Next();
    }

    index_map.clear();
    i = 0;
    index_map[i++] = 3;
    index_map[i++] = 2;
    index_map[i++] = 1;
    index_map[i++] = 5;
    index_map[i++] = 4;
    std::cout << "\n\n";
    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=7
    for (int i = 0; i < 5 && it->Valid(); i++) {
        mulitKey = it->getMultiKey();
        ASSERT_STREQ(mulitKey.key().c_str(), kv.at(i).first.c_str()) <<
            "i = " << i << " ver=" << mulitKey.ver() << std::endl;
        ASSERT_STREQ(it->value().c_str(), kv.at(i).second.c_str()) <<
            "i = " << i << " ver=" << mulitKey.ver() << std::endl;
        ASSERT_EQ(mulitKey.ver(), index_map[i]) <<
            "i = " << i << " key=" << mulitKey.key() << std::endl;
        it->Traverse();
    }
    /*
    db_->Delete(k);

    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));
    ASSERT_FALSE(it->Valid());

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));
    ASSERT_TRUE(it->Valid());

    mulitKey = it->getMultiKey();
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_TRUE(mulitKey.is_del());
    ASSERT_EQ(mulitKey.ver(), 2);

    it->Traverse();
    ASSERT_TRUE(it->Valid());
    mulitKey = it->getMultiKey();
    ASSERT_STREQ(mulitKey.key().c_str(), "a");
    ASSERT_STREQ(it->value().c_str(), "a");
    ASSERT_FALSE(mulitKey.is_del());
    ASSERT_EQ(mulitKey.ver(), 1);
    */
} //TEST_F end
} // namespace end

