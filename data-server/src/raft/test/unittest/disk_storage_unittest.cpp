#include <gtest/gtest.h>

#include "base/util.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "raft/src/impl/storage/storage_disk.h"
#include "raft/src/impl/storage_reader.h"
#include "test_util.h"
#include "base/status.h"

using Status = sharkstore::Status;

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace sharkstore {
namespace test {

using namespace sharkstore::raft::impl;
using namespace sharkstore::raft::impl::storage;
using namespace sharkstore::raft::impl::testutil;

class StorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        char path[] = "/tmp/sharkstore_raft_storage_test_XXXXXX";
        char* tmp = mkdtemp(path);
        ASSERT_TRUE(tmp != NULL);
        tmp_dir_ = tmp;

        ops_.log_file_size = 1024;
        ops_.allow_corrupt_startup = true;

        Open();
    }

    void ReOpen() {
        auto s = storage_->Close();
        ASSERT_TRUE(s.ok()) << s.ToString();
        delete storage_;

        ops_.initial_first_index = 0;

        Open();
    }

    void TearDown() override {
        if (storage_ != nullptr) {
            auto s = storage_->Destroy(false);
            ASSERT_TRUE(s.ok()) << s.ToString();
            delete storage_;
        }
    }

    void LimitMaxLogs(size_t size) {
        ops_.max_log_files = size;
        ReOpen();
    }

    VecLogFile&& GetLogFiles() {
        return std::move(storage_->log_files_);
    }
    VecLogFile&& GetTruncFiles() {
        return std::move(storage_->log_files_truncated_);
    }

    Status LoadFromDiskStorage() {
        auto& log_file = storage_reader_->log_files_;
        Status s;
        if (storage_->CommitFileCount() == 0) {
            s = storage_->LoadCommitFiles(1);
            //ASSERT_TRUE(s.ok()) << s.ToString();
        }
        for (const auto& f : storage_->GetCommitFiles()) {
            log_file.emplace(f);
        }
        storage_reader_->listLogs();

        return Status::OK();
    }

private:
    void Open() {
        storage_ = new DiskStorage(1, tmp_dir_, ops_, 
                [&](uint64_t& index) { 
                index = storage_->Applied(); return Status::OK(); 
                });
        auto s = storage_->Open();
        ASSERT_TRUE(s.ok()) << s.ToString();

        s = initReader();
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    Status initReader() {
        storage_reader_ = new StorageReader(1, 1, nullptr);
        return Status::OK();
    }

protected:
    std::string tmp_dir_;
    DiskStorage::Options ops_; // open options
    DiskStorage* storage_;
    StorageReader* storage_reader_;
};

class StorageHoleTest : public StorageTest {
protected:
    void SetUp() override {
        ops_.initial_first_index = 100;
        StorageTest::SetUp();
    }
};

TEST_F(StorageTest, LogEntry) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    // 一条一条取
    for (uint64_t index = lo; index < hi; ++index) {
        std::vector<EntryPtr> ents;
        bool compacted = false;
        s = storage_->Entries(index, index + 1, std::numeric_limits<uint64_t>::max(),
                &ents, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(ents.size(), 1);
        s = Equal(ents[0], to_writes[index-lo]);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    // 读取全部
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 测试term接口
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - 1]->term());
    }

    // 带maxsize
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents2(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 至少一条
    ents.clear();
    s = storage_->Entries(lo, hi, 1, &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, std::vector<EntryPtr>{to_writes[0]});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 读取被截断的日志
    ents.clear();
    s = storage_->Entries(0, hi, std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    ASSERT_TRUE(ents.empty());

    // 关闭重新打开
    ReOpen();

    // 测试起始index
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    // 读取全部日志
    ents.clear();
    compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 测试term接口
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - 1]->term());
    }

    // 带maxsize
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents3(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents3);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Conflict) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto entry = RandomEntry(50, 256);
    s = storage_->StoreEntries(std::vector<EntryPtr>{entry});
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 50);

    // 读取全部
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, 51, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents2(to_writes.begin(), to_writes.begin() + 49);
    ents2.push_back(entry);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Snapshot) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    pb::SnapshotMeta meta;
    meta.set_index(sharkstore::randomInt() + 100);
    meta.set_term(sharkstore::randomInt());
    s = storage_->ApplySnapshot(meta);
    ASSERT_TRUE(s.ok()) << s.ToString();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(index, meta.index() + 1);
    s = storage_->LastIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(index, meta.index());

    uint64_t term = 0;
    bool compacted = false;
    s = storage_->Term(meta.index() - 20, &term, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    s = storage_->Term(meta.index(), &term, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(term, meta.term());
    ASSERT_FALSE(compacted);

    auto e = RandomEntry(meta.index() + 1);
    s = storage_->StoreEntries(std::vector<EntryPtr>{e});
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<EntryPtr> ents;
    s = storage_->Entries(meta.index() + 1, meta.index() + 2,
                          std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, std::vector<EntryPtr>{e});
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, KeepCount) {
    LimitMaxLogs(3);
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    storage_->AppliedTo(99);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto count = storage_->FilesCount();
    auto e = RandomEntry(100);
    s = storage_->StoreEntries(std::vector<EntryPtr>{e});
    if (!s.ok()) {
        std::cout << "StoreEntries error: " << s.ToString().c_str() << std::endl;
    }
    auto count2 = storage_->FilesCount();
   
    std::cout << count << ", " << count2 << std::endl;
    ASSERT_LT(count2, count);
    ASSERT_GE(count2, 3);

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::cout << "First: " << index << std::endl;

    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    ReOpen();
    std::vector<EntryPtr> ents2;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents2,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Destroy) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    s = storage_->Destroy(false);
    ASSERT_TRUE(s.ok()) << s.ToString();
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = ::stat(tmp_dir_.c_str(), &sb);
    ASSERT_EQ(ret, -1);
    ASSERT_EQ(errno, ENOENT);
}

TEST_F(StorageTest, DestroyBak) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto start = time(NULL);

    s = storage_->Destroy(true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto end = time(NULL);

    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    int ret = ::stat(tmp_dir_.c_str(), &sb);
    ASSERT_EQ(ret, -1);
    ASSERT_EQ(errno, ENOENT);

    // find backup path
    std::string bak_path;
    for (auto t = start; t <= end; ++t) {
        std::string path = tmp_dir_ + ".bak." + std::to_string(t);
        ret = ::stat(path.c_str(), &sb);
        if (ret == 0) {
            bak_path = path;
            break;
        }
    }
    ASSERT_TRUE(!bak_path.empty());

    // load entries from backup
    DiskStorage bds(1, bak_path, DiskStorage::Options(), [](uint64_t& index) { index = 0; return Status::OK();});
    s = bds.Open();
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::vector<EntryPtr > ents;
    bool compacted = false;
    s = bds.Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents, &compacted) ;
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

#ifndef NDEBUG  // debug模式下才开启
TEST_F(StorageTest, Corrupt1) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 最后一个日志文件上添加损坏块
    storage_->TEST_Add_Corruption1();

    // 读取全部
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 重新打开再读取
    ReOpen();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 再写再读
    RandomEntries(hi, hi + 10, 256, &to_writes);
    s = storage_->StoreEntries(
        std::vector<EntryPtr>(to_writes.begin() + hi - 1, to_writes.end()));
    ASSERT_TRUE(s.ok()) << s.ToString();
    hi += 10;
    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
}

TEST_F(StorageTest, Corrupt2) {
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 最后一个日志文件上添加损坏块
    storage_->TEST_Add_Corruption2();

    // 重新打开
    ReOpen();

    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 1);
    s = storage_->LastIndex(&index);
    ASSERT_LT(index, 99);
    ASSERT_GE(index, 1);
    while (to_writes.size() > index) {
        to_writes.pop_back();
    }

    hi = index + 1;
    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 再写再读
    RandomEntries(hi, hi + 10, 256, &to_writes);
    s = storage_->StoreEntries(
        std::vector<EntryPtr>(to_writes.begin() + hi - 1, to_writes.end()));
    ASSERT_TRUE(s.ok()) << s.ToString();
    hi += 10;
    ents.clear();
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();
}
#endif

TEST_F(StorageHoleTest, StartIndex) {
    uint64_t index = 0;
    auto s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, 100);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, 99);

    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(99, 200, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);

    const uint64_t lo = 100, hi = 200;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    s = storage_->StoreEntries(to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, lo);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, hi - 1);

    // 读取全部
    ents.clear();
    compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 测试term接口
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - lo]->term());
    }

    // 带maxsize
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents2(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 至少一条
    ents.clear();
    s = storage_->Entries(lo, hi, 1, &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, std::vector<EntryPtr>{to_writes[0]});
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 读取被截断的日志
    ents.clear();
    s = storage_->Entries(0, hi, std::numeric_limits<uint64_t>::max(), &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(compacted);
    ASSERT_TRUE(ents.empty());

    // 关闭重新打开
    ReOpen();

    // 测试起始index
    s = storage_->FirstIndex(&index);
    ASSERT_EQ(index, lo);
    s = storage_->LastIndex(&index);
    ASSERT_EQ(index, hi - 1);

    // 读取全部日志
    ents.clear();
    compacted = false;
    s = storage_->Entries(lo, hi, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    s = Equal(ents, to_writes);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // 测试term接口
    for (uint64_t i = lo; i < hi; ++i) {
        uint64_t term = 0;
        bool compacted = false;
        s = storage_->Term(i, &term, &compacted);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_FALSE(compacted);
        ASSERT_EQ(term, to_writes[i - lo]->term());
    }

    // 带maxsize
    ents.clear();
    s = storage_->Entries(lo, hi,
                          to_writes[0]->ByteSizeLong() + to_writes[1]->ByteSizeLong(),
                          &ents, &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);
    std::vector<EntryPtr> ents3(to_writes.begin(), to_writes.begin() + 2);
    s = Equal(ents, ents3);
    ASSERT_TRUE(s.ok()) << s.ToString();
}


TEST_F(StorageTest, GetFromRaftLogFile) {
    LimitMaxLogs(3);
    uint64_t lo = 1, hi = 100;
    std::vector<EntryPtr> to_writes;
    RandomEntries(lo, hi, 256, &to_writes);
    auto s = storage_->StoreEntries(to_writes);
    storage_->AppliedTo(99);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto count = storage_->FilesCount();
    auto e = RandomEntry(100);
    s = storage_->StoreEntries(std::vector<EntryPtr>{e});
    if (!s.ok()) {
        std::cout << "StoreEntries error: " << s.ToString().c_str() << std::endl;
    }
    //file_size >= 1024 就切换生成新的文件
    auto count2 = storage_->FilesCount();
   
    const uint64_t apply_index = 0;
    uint64_t start_index{0};
    s = storage_->LoadCommitFiles(apply_index);
    assert(s.ok());
    for (const auto& f : storage_->GetCommitFiles()) {
        if (start_index == 0) {
            start_index = f->Index();
        }
        std::cout << f->Path().c_str() << ":" << f->GetFullFlag() << std::endl;
    }

    std::shared_ptr<raft_cmdpb::Command> cmd = nullptr;
    s = LoadFromDiskStorage();
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = storage_reader_->GetData(start_index, cmd);
    ASSERT_TRUE(s.ok()) << s.ToString();

    /*
    std::cout << "iterator all log_files\n";
    for (const auto& f : GetLogFiles()) {
        std::cout << f->Path().c_str() << ":" << f->GetFullFlag() << std::endl;
    }
    std::cout << "iterator all log_files_truncated_\n";
    for (const auto& f : GetTruncFiles()) {
        std::cout << f->Path().c_str() << ":" << f->GetFullFlag() << std::endl;
    }
    */

    std::cout << count << ", " << count2 << std::endl;
    ASSERT_LT(count2, count);
    ASSERT_GE(count2, 3);


    uint64_t index = 0;
    s = storage_->FirstIndex(&index);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::cout << "First: " << index << std::endl;

    std::vector<EntryPtr> ents;
    bool compacted = false;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    ReOpen();
    std::vector<EntryPtr> ents2;
    s = storage_->Entries(index, 101, std::numeric_limits<uint64_t>::max(), &ents2,
                          &compacted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_FALSE(compacted);

    s = Equal(ents, ents2);
    ASSERT_TRUE(s.ok()) << s.ToString();
}


} /* namespace test */
} /* namespace sharkstore */
