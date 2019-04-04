#include <iostream>
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <gtest/gtest.h>
#include "storage/db/multi_v_key.h"
#include "helper/mock/mass_tree_mvcc_mock.h"
#include "helper/mock/mass_tree_iterator_mock.h"

using namespace sharkstore::test::mock;
using namespace sharkstore::dataserver::storage;

struct KeyStatus {
   std::string key;
   std::string val;
   uint64_t ver;
   bool del_flag;

   KeyStatus(std::string k, std::string v, uint64_t version, bool flag) :
   key(k),val(v),ver(version),del_flag(flag) {}
};

struct IndexVer {
    int index;
    uint64_t max_ver;
    IndexVer(int inx, uint64_t ver) :
    index(inx),max_ver(ver) {}
};

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

/*
 * put=>a1=>check=>delete=>a3,a1=>check(a3,a1)
 *
 * example:
 * 1.generate key=>a
 * 2.put=>a1
 * 3.iterator,check key=>a1
 * 4.delete key=>a3=>keylist:a3,a1
 * 5.iterator,check key=>"a" not exists
 * 6.origin iterator
 * 6.1check first key a3
 * 6.2cursor next
 * 6.3check second key a1
*/
TEST_F(MassMvccTest, SingleKey) {
    auto db = getenv("DB");
    ASSERT_STREQ(db, "mass-tree-mock");

    MultiVersionKey multiKey;
    std::string k = "a";
    std::string v = "a";
    db_->Put(k, v);//ver=1

    std::unique_ptr<MassTreeIteratorMock> it(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=2
    ASSERT_TRUE(it->Valid());
    multiKey = it->getMultiKey();
    //std::cout << multiKey.key() << " ver=" << multiKey.ver() << "::::" <<it->key() << std::endl;
    ASSERT_STREQ(multiKey.key().c_str(), "a");
    ASSERT_STREQ(it->value().c_str(), "a");
    ASSERT_EQ(multiKey.ver(), 1);

    db_->Delete(k);//ver=3

    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=4
    //multiKey = it->getMultiKey();
    //std::cout << multiKey.key() << " ver=" << multiKey.ver() << "::::" <<it->key() << std::endl;
    ASSERT_FALSE(it->Valid());

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=5
    ASSERT_TRUE(it->Valid());

    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), "a");
    ASSERT_TRUE(multiKey.is_del());
    ASSERT_EQ(multiKey.ver(), 3);

    it->Traverse();
    ASSERT_TRUE(it->Valid());
    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), "a");
    ASSERT_STREQ(it->value().c_str(), "a");
    ASSERT_FALSE(multiKey.is_del());
    ASSERT_EQ(multiKey.ver(), 1);
}

/*
 * put=>a3,a2,a1,b5,b4
 * iterator check(a3,b5)
 * origin iterator check(a3,a2,a1,b5,b4)
 * delete "a"=>a8,a3,a2,a1,b5,b4=>
 * iterator check(b5)
 * origin iterator check(a8,a3,a2,a1,b5,b4)
 */
TEST_F(MassMvccTest, MultiKey) {
    auto db = getenv("DB");
    ASSERT_STREQ(db, "mass-tree-mock");

    MultiVersionKey multiKey;

    std::vector<KeyStatus> kv;
    kv.emplace_back("a", "va", 1, false);
    kv.emplace_back("a", "va", 2, false);
    kv.emplace_back("a", "va", 3, false);
    kv.emplace_back("b", "vb", 4, false);
    kv.emplace_back("b", "vb", 5, false);

    int i = 0;
    db_->Put(kv.at(i).key, kv.at(i).val);//ver=1
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=2
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=3
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=4
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=5

    std::vector<IndexVer> index_map;
    index_map.emplace_back(2, 3);//kv[2]=>a3
    index_map.emplace_back(4, 5);//kv[4]=>b5
    std::unique_ptr<MassTreeIteratorMock> it(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=6
    for (int i = 0; i < 2 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    index_map.clear();
    index_map.emplace_back(0, 3);
    index_map.emplace_back(1, 2);
    index_map.emplace_back(2, 1);
    index_map.emplace_back(3, 5);
    index_map.emplace_back(4, 4);
    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=7
    for (int i = 0; i < 5 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv[i].key.c_str()) <<
            "i = " << i << " ver=" << multiKey.ver() << std::endl;
        ASSERT_STREQ(it->value().c_str(), kv[i].val.c_str()) <<
            "i = " << i << " ver=" << multiKey.ver() << std::endl;
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver) <<
            "i = " << i << " key=" << multiKey.key() << std::endl;
        it->Traverse();
    }

    kv.emplace_back("a", "va", 8, true);
    index_map.emplace(index_map.begin(), 5, 8);
    db_->Delete(kv[0].key);//ver=8,del a

    //b5
    db_->seek_set(true);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=9
    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), kv[4].key.c_str());
    ASSERT_STREQ(it->value().c_str(), kv[4].val.c_str());
    ASSERT_EQ(multiKey.ver(), 5);//b5

    //a8,a3,a2,a1,b5,b4
    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock*>(db_->NewIterator("", "")));//ver=10
    for (int i = 0; i < 6 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv[index_map[i].index].key.c_str()) <<
            "i = " << i << " ver=" << multiKey.ver() << std::endl;
        if (multiKey.is_del()) {
            ASSERT_STREQ(it->value().c_str(), "") <<
            "i = " << i << " ver=" << multiKey.ver() << std::endl;
        } else {
            ASSERT_STREQ(it->value().c_str(), kv[index_map[i].index].val.c_str()) <<
            "i = " << i << " ver=" << multiKey.ver() << std::endl;
        }
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver) <<
            "i = " << i << " key=" << multiKey.key() << std::endl;
        it->Traverse();
    }
} //TEST_F end

/*
 * put=>a3,a2,a1,b6,b5,b4,c9,c8,c7
 * reset version to 4
 * iterator=>version=5, check result=>a3,b5
 * reset version to 7
 * iterator=>version=8, check result=>a3,b5,c8
 */
TEST_F(MassMvccTest, Snapshot) {
    MultiVersionKey multiKey;

    std::vector<KeyStatus> kv;
    kv.emplace_back("a", "va", 1, false);
    kv.emplace_back("a", "va", 2, false);
    kv.emplace_back("a", "va", 3, false);
    kv.emplace_back("b", "vb", 4, false);
    kv.emplace_back("b", "vb", 5, false);
    kv.emplace_back("b", "vb", 6, false);
    kv.emplace_back("c", "vc", 7, false);
    kv.emplace_back("c", "vc", 8, false);
    kv.emplace_back("c", "vc", 9, false);

    int i = 0;
    db_->Put(kv.at(i).key, kv.at(i).val);//ver=1
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=2
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=3
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=4
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=5
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=6
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=7
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=8
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=9

    db_->StoreVersion(4);//ver=4

    std::vector<IndexVer> index_map;
    index_map.emplace_back(2, 3);//kv[2]=>a3
    index_map.emplace_back(4, 5);//kv[4]=>b5
    std::unique_ptr<MassTreeIteratorMock> it(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=5
    for (int i = 0; i < 2 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    db_->StoreVersion(7);//ver=7

    index_map.clear();
    index_map.emplace_back(2, 3);//kv[2]=>a3
    index_map.emplace_back(5, 6);//kv[5]=>b5
    index_map.emplace_back(7, 8);//kv[7]=>c8
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=8
    for (int i = 0; i < 3 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }
}

/*
 * put=>a1
 * iterator check(a1)
 * scrub();
 * iterator check(a1)//scrub no impact
 * delete "a"=>a4-del,a1
 * scrub();=>a clear
 * iterator check=>no data
 * origin iterator check=>no data
 * reset version,version=0
 * put=>a3,a2,a1
 * scrub();
 * origin iterator check(a3,a2,a1)
 * put=>a6,a5,a3,a2,a1
 * delete "a"=>a7-del,a6,a5,a3,a2,a1
 * scrub();=>clear a
 * origin iterator check=>no data
 */
TEST_F(MassMvccTest, SingleKeyDelScrub) {
    MultiVersionKey multiKey;

    std::vector<KeyStatus> kv;
    kv.emplace_back("a", "va", 1, false);

    int i = 0;
    db_->Put(kv.at(i).key, kv.at(i).val);//ver=1

    //std::unique_ptr<Vec3> v1 = std::make_unique<Vec3>();
    //single key=>scrub=>single key exists
    //std::unique_ptr<MassTreeIteratorMock> it = std::make_unique<MassTreeIteratorMock>(
    //        static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));
    std::unique_ptr<MassTreeIteratorMock> it(
           static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=2
    ASSERT_TRUE(it->Valid());
    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), "a") <<
        "i = " << i << " ver=" << multiKey.ver() << std::endl;
    ASSERT_STREQ(it->value().c_str(), "va") <<
        "i = " << i << " ver=" << multiKey.ver() << std::endl;
    ASSERT_EQ(multiKey.ver(), 1) <<
        "i = " << i << " key=" << multiKey.key() << std::endl;

    db_->Scrub();

    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=3
    ASSERT_TRUE(it->Valid());
    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), "a") <<
        "i = " << i << " ver=" << multiKey.ver() << std::endl;
    ASSERT_STREQ(it->value().c_str(), "va") <<
        "i = " << i << " ver=" << multiKey.ver() << std::endl;
    ASSERT_EQ(multiKey.ver(), 1) <<
        "i = " << i << " key=" << multiKey.key() << std::endl;

    //single key=>del=>scrub=>single key not exists
    db_->Delete("a");//ver=4
    it.reset(nullptr);
    db_->Scrub();
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=5
    ASSERT_FALSE(it->Valid());

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=6
    ASSERT_FALSE(it->Valid());

    //single multi version key=>scrub=>key exists
    db_->StoreVersion(0);//ver=0
    kv.emplace_back("a", "va", 2, false);
    kv.emplace_back("a", "va", 3, false);
    i = 0;
    db_->Put(kv.at(i).key, kv.at(i).val);//ver=1
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=2
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=3

    db_->Scrub();

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=4
    ASSERT_TRUE(it->Valid());
    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), "a") <<
        "i = " << i << " ver=" << multiKey.ver() << std::endl;
    ASSERT_STREQ(it->value().c_str(), "va") <<
        "i = " << i << " ver=" << multiKey.ver() << std::endl;
    ASSERT_EQ(multiKey.ver(), 3) <<
        "i = " << i << " key=" << multiKey.key() << std::endl;

    it->Traverse();
    ASSERT_FALSE(it->Valid());

    //single multi version key=>del=>scrub=>key clear
    it.reset(nullptr);
    kv.emplace_back("a", "va", 5, false);
    kv.emplace_back("a", "va", 6, false);
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=5
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=6

    db_->Delete("a");
    db_->Scrub();

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=4
    ASSERT_FALSE(it->Valid());
}

/*
 * put=>a1,b2,c3
 * scrub();
 * iterator check(a1,b2,c3)
 * delete "a"=>a5-del,a1,b2,c3
 * scrub();=>b2,c3
 * delete "c"=>b2,c8-del,c3
 * scrub();=>b2
 * iterator check(b2)
 * origin iterator check(b2)
 * put=>a13,a12,a11,b15,b14,b2,c18,c17,c16
 * scrub();
 * iterator check(a13,b15,c18)
 * origin iterator check(a13,b15,c18)
 * put=>a23,a22,a21,a13,b25,b24,b15,c28,c27,c26,c18
 * reset version,version=18
 * iterator=>version=19
 * scrub();=>a23,a22,a21,b25,b24,c28,c27,c26
 * iterator check(a23,b25,c28)
 * origin iterator check(a23,a22,a21,b25,b24,c28,c27,c26
 * reset version,version=28
 * delete "c"
 * put=>a30,a23,a22,a21,b31,b25,b24,c29-del,c28,c27,c26
 * reset version,version=28
 * iterator=>version=29
 * scrub();=>a30,b31,c29-del
 * reset version,version=31
 * iterator check(a30,b31,c29-del)
 * put=>a33,a30,b34,b31,c29-del
 * reset version,version=29
 * iterator=>version=30
 * scrub();=>a33,a30,b34,b31
 * reset version,version=34
 * origin iterator check(a33,a30,b34,b31)
 */
TEST_F(MassMvccTest, MultiKeyDelScrub) {
    MultiVersionKey multiKey;

    std::vector<KeyStatus> kv;
    kv.emplace_back("a", "va", 1, false);
    kv.emplace_back("b", "vb", 2, false);
    kv.emplace_back("c", "vc", 3, false);

    int i = 0;
    db_->Put(kv.at(i).key, kv.at(i).val);//ver=1
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=2
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=3

    db_->Scrub();

    //multi key single version
    std::vector<IndexVer> index_map;
    index_map.emplace_back(0, 1);//kv[0]=>a1
    index_map.emplace_back(1, 2);//kv[1]=>b2
    index_map.emplace_back(2, 3);//kv[2]=>c3
    std::unique_ptr<MassTreeIteratorMock> it(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=4
    for (int i = 0; i < 3 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    db_->Delete("a");//ver=5
    it.reset(nullptr);
    db_->Scrub();

    index_map.clear();
    index_map.emplace_back(1, 2);//kv[1]=>b2
    index_map.emplace_back(2, 3);//kv[2]=>c3
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=6
    for (int i = 0; i < 2 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    index_map.clear();
    index_map.emplace_back(1, 2);//kv[1]=>b2
    index_map.emplace_back(2, 3);//kv[2]=>c3
    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=7
    for (int i = 0; i < 2 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    db_->Delete("c");//ver=8
    it.reset(nullptr);
    db_->Scrub();

    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=9
    ASSERT_TRUE(it->Valid());
    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), "b");
    ASSERT_STREQ(it->value().c_str(), "vb");
    ASSERT_EQ(multiKey.ver(), 2);
    it->Next();
    ASSERT_FALSE(it->Valid());

    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=10
    multiKey = it->getMultiKey();
    ASSERT_STREQ(multiKey.key().c_str(), "b");
    ASSERT_STREQ(it->value().c_str(), "vb");
    ASSERT_EQ(multiKey.ver(), 2);
    it->Next();
    ASSERT_FALSE(it->Valid());

    //multi key multi version
    it.reset(nullptr);
    kv.emplace_back("a", "va", 11, false);
    kv.emplace_back("a", "va", 12, false);
    kv.emplace_back("a", "va", 13, false);
    kv.emplace_back("b", "vb", 14, false);
    kv.emplace_back("b", "vb", 15, false);
    kv.emplace_back("c", "vc", 16, false);
    kv.emplace_back("c", "vc", 17, false);
    kv.emplace_back("c", "vc", 18, false);
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=11,i=3
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=12,i=4
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=13
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=14
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=15
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=16
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=17
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=18,i=10

    db_->Scrub();

    index_map.clear();
    index_map.emplace_back(5, 13);//kv[5]=>a13
    index_map.emplace_back(7, 15);//kv[7]=>b15
    index_map.emplace_back(10, 18);//kv[10]=>c18
    db_->seek_set(true);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=19
    for (int i = 0; i < 3 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    index_map.clear();
    index_map.emplace_back(5, 13);//kv[5]=>a13
    index_map.emplace_back(7, 15);//kv[7]=>b15
    index_map.emplace_back(10, 18);//kv[10]=>c18
    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=20
    for (int i = 0; i < 9 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Traverse();
    }

    //a13,b15,c18
    it.reset(nullptr);
    kv.emplace_back("a", "va", 21, false);
    kv.emplace_back("a", "va", 22, false);
    kv.emplace_back("a", "va", 23, false);
    kv.emplace_back("b", "vb", 24, false);
    kv.emplace_back("b", "vb", 25, false);
    kv.emplace_back("c", "vc", 26, false);
    kv.emplace_back("c", "vc", 27, false);
    kv.emplace_back("c", "vc", 28, false);
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=21,i=11
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=22,i=12
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=23,i=13
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=24,i=14
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=25,i=15
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=26,i=16
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=27,i=17
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=28,i=18

    //a23,a22,a21,a13,b25,b24,b15,c28,c27,c26,c18
    db_->StoreVersion(18);//ver=18
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=19
    //a23,a22,a21,b25,b24,c28,c27,c26
    db_->Scrub();

    index_map.clear();
    index_map.emplace_back(13, 23);//kv[13]=>a23
    index_map.emplace_back(15, 25);//kv[15]=>b25
    index_map.emplace_back(18, 28);//kv[18]=>c28
    db_->seek_set(true);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=18
    for (int i = 0; i < 3 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    index_map.clear();
    index_map.emplace_back(13, 23);//kv[13]=>a23
    index_map.emplace_back(12, 22);//kv[12]=>a22
    index_map.emplace_back(11, 21);//kv[11]=>a21
    index_map.emplace_back(15, 25);//kv[15]=>b25
    index_map.emplace_back(14, 24);//kv[14]=>b24
    index_map.emplace_back(18, 28);//kv[18]=>c28
    index_map.emplace_back(17, 27);//kv[17]=>c27
    index_map.emplace_back(16, 26);//kv[16]=>c26
    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=30
    for (int i = 0; i < 9 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Traverse();
    }

    //a23,a22,a21,b25,b24,c28,c27,c26
    db_->StoreVersion(28);//ver=28

    db_->Delete("c");//ver=29
    // a23,a22,a21,b25,b24,c29,c28,c27,c26
    kv.emplace_back("c", "vc", 29, true);
    i++;//i=19

    kv.emplace_back("a", "va", 30, false);
    kv.emplace_back("b", "vb", 31, false);
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=30,i=20
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=31,i=21
    //a30,a23,a22,a21,b31,b25,b24,c29,c28,c27,c26
    db_->StoreVersion(28);//ver=28
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=29
    db_->Scrub();
    //a30,b31,c29

    db_->StoreVersion(31);//ver=31
    index_map.clear();
    index_map.emplace_back(20, 30);//kv[19]=>a30
    index_map.emplace_back(21, 31);//kv[21]=>b31
    index_map.emplace_back(19, 29);//kv[19]=>c29
    db_->seek_set(true);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=32
    for (int i = 0; i < 2 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Next();
    }

    ASSERT_FALSE(it->Valid());
    //test a33,a30,b34,b31,c29-del
    kv.emplace_back("a", "va", 33, false);
    kv.emplace_back("b", "vb", 34, false);
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=33,i=22
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=34,i=23
    db_->StoreVersion(29);//ver=29
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=30
    db_->Scrub();
    //test a33,a30,b34,b31

    db_->StoreVersion(34);//ver=34
    index_map.clear();
    index_map.emplace_back(22, 33);//kv[22]=>a33
    index_map.emplace_back(20, 30);//kv[20]=>a30
    index_map.emplace_back(23, 34);//kv[23]=>b34
    index_map.emplace_back(21, 31);//kv[21]=>c31
    db_->seek_set(false);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=35
    for (int i = 0; i < 4 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        it->Traverse();
    }
    ASSERT_FALSE(it->Valid());
}

/*
 * put=>a1,b2,c3
 * delete "c"=>a1,b2,c4-del,c3
 * delete "c"=>a1,b2,c5-del,c4-del,c3
 * origin iterator check(a1,b2,c5-del,c4-del,c3)
 * scrub();
 * origin iterator check(a1,b2)
 */
TEST_F(MassMvccTest, MultiKeyDelBound) {
    MultiVersionKey multiKey;
    std::vector<IndexVer> index_map;

    std::vector<KeyStatus> kv;
    kv.emplace_back("a", "va", 1, false);
    kv.emplace_back("b", "vb", 2, false);
    kv.emplace_back("c", "vc", 3, false);

    int i = 0;
    db_->Put(kv.at(i).key, kv.at(i).val);//ver=1,i=0
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=2,i=1
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=3,i=2

    db_->Delete("c");//ver=4
    db_->Delete("c");//ver=5,a1,b2,c5-del,c4-del,c3
    kv.emplace_back("c", "", 4, true);
    kv.emplace_back("c", "", 5, true);
    i++;
    i++;

    db_->seek_set(false);
    std::unique_ptr<MassTreeIteratorMock> it(
            static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=6
    index_map.emplace_back(0, 1);//kv[0]=>a1
    index_map.emplace_back(1, 2);//kv[1]=>b2
    index_map.emplace_back(4, 5);//kv[4]=>c5
    index_map.emplace_back(3, 4);//kv[3]=>c4
    index_map.emplace_back(2, 3);//kv[2]=>c3
    for (int i = 0; i < 5 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        ASSERT_EQ(multiKey.is_del(), kv.at(index_map[i].index).del_flag);
        it->Traverse();
    }
    ASSERT_FALSE(it->Valid());

    db_->Scrub();

    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=8
    index_map.clear();
    index_map.emplace_back(0, 1);//kv[0]=>a1
    index_map.emplace_back(1, 2);//kv[1]=>b2
    for (int i = 0; i < 2 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        ASSERT_EQ(multiKey.is_del(), kv.at(index_map[i].index).del_flag);
        it->Traverse();
    }
    ASSERT_FALSE(it->Valid());
}

/*
 * put=>a1,b2
 * delete "c"=>a1,b2,c3-del
 * delete "c"=>a1,b2,c4-del,c3-del
 * origin iterator check(a1,b2,c4-del,c3-del)
 * reset version,version=3
 * iterator=>version=4
 * scrub();=>a1,b2,c4-del
 * origin iterator check(a1,b2,c4-del)
 * scrub();=>a1,b2
 * origin iterator check(a1,b2)
 */
TEST_F(MassMvccTest, MultiKeyDelNotExistsBound) {
    MultiVersionKey multiKey;
    std::vector<IndexVer> index_map;

    std::vector<KeyStatus> kv;
    kv.emplace_back("a", "va", 1, false);
    kv.emplace_back("b", "vb", 2, false);

    int i = 0;
    db_->Put(kv.at(i).key, kv.at(i).val);//ver=1,i=0
    i++;db_->Put(kv.at(i).key, kv.at(i).val);//ver=2,i=1

    db_->Delete("c");//ver=3
    db_->Delete("c");//ver=4,a1,b2,c4-del,c3-del
    kv.emplace_back("c", "", 3, true);
    kv.emplace_back("c", "", 4, true);
    i++;
    i++;

    db_->seek_set(false);
    std::unique_ptr<MassTreeIteratorMock> it(
            static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=5
    index_map.emplace_back(0, 1);//kv[0]=>a1
    index_map.emplace_back(1, 2);//kv[1]=>b2
    index_map.emplace_back(3, 4);//kv[3]=>c4
    index_map.emplace_back(2, 3);//kv[2]=>c3
    for (int i = 0; i < 4 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        ASSERT_EQ(multiKey.is_del(), kv.at(index_map[i].index).del_flag);
        it->Traverse();
    }
    ASSERT_FALSE(it->Valid());

    db_->StoreVersion(3);
    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=4
    db_->Scrub();

    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=5
    index_map.clear();
    index_map.emplace_back(0, 1);//kv[0]=>a1
    index_map.emplace_back(1, 2);//kv[1]=>b2
    index_map.emplace_back(3, 4);//kv[3]=>c4
    for (int i = 0; i < 3 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        ASSERT_EQ(multiKey.is_del(), kv.at(index_map[i].index).del_flag);
        it->Traverse();
    }
    ASSERT_FALSE(it->Valid());

    db_->Scrub();

    it.reset(static_cast<MassTreeIteratorMock *>(db_->NewIterator("", "")));//ver=6
    index_map.clear();
    index_map.emplace_back(0, 1);//kv[0]=>a1
    index_map.emplace_back(1, 2);//kv[1]=>b2
    for (int i = 0; i < 2 && it->Valid(); i++) {
        multiKey = it->getMultiKey();
        ASSERT_STREQ(multiKey.key().c_str(), kv.at(index_map[i].index).key.c_str());
        ASSERT_STREQ(it->value().c_str(), kv.at(index_map[i].index).val.c_str());
        ASSERT_EQ(multiKey.ver(), index_map[i].max_ver);
        ASSERT_EQ(multiKey.is_del(), kv.at(index_map[i].index).del_flag);
        it->Traverse();
    }
    ASSERT_FALSE(it->Valid());
}

} // namespace end

