_Pragma("once");

#include <thread>
#include <mutex>
#include <condition_variable>

#include "storage/db/db_interface.h"
#include "storage/db/mvcc.h"
#include "mass_tree_db.h"

namespace sharkstore { namespace test { namespace mock { class MvccMassTreeMock; }}}

namespace sharkstore {
namespace dataserver {
namespace storage {

class MvccMassTree : public DbInterface {
public:
    friend class sharkstore::test::mock::MvccMassTreeMock;

    MvccMassTree() = default;
    ~MvccMassTree();

    struct MvccTree {
        MassTreeDB tree;
        Mvcc mvcc;
    };

    bool IsInMemory() override { return true; }

    Status Open() override;

    Status Get(const std::string &key, std::string *value) override;
    Status Get(void *column_family, const std::string &key, std::string *value) override;

    Status Put(const std::string &key, const std::string &value) override;
    Status Put(void *column_family, const std::string &key, const std::string &value) override;

    std::unique_ptr<WriteBatchInterface> NewBatch() override;
    Status Write(WriteBatchInterface *batch) override;

    Status Delete(const std::string &key) override;
    Status Delete(void *column_family, const std::string &key) override;

    Status DeleteRange(void *column_family, const std::string &begin_key, const std::string &end_key) override;

    void *DefaultColumnFamily() override;
    void *TxnCFHandle() override;

    IteratorInterface *NewIterator(const std::string &start, const std::string &limit) override;
    Status NewIterators(std::unique_ptr<IteratorInterface> &data_iter,
                        std::unique_ptr<IteratorInterface> &txn_iter,
                        const std::string &start, const std::string &limit) override;

    void GetProperty(const std::string &k, std::string *v) override;

    Status SetOptions(void *column_family, const std::unordered_map<std::string, std::string> &new_options) override;

    Status SetDBOptions(const std::unordered_map<std::string, std::string> &new_options) override;

    std::string GetMetrics();

private:
    Status get(MvccTree *family, const std::string &key, std::string *value);
    Status put(MvccTree *family, const std::string &key, const std::string &value);
    Status del(MvccTree *family, const std::string &key);
    Status deleteRange(MvccTree *family, const std::string& begin_key, const std::string& end_key);
    IteratorInterface *newIter(MvccTree *family, const std::string &start, const std::string &limit);

    void runGC();
    void scrub(MvccTree *family);


private:
    const size_t gc_interval_msec_ = 2000; // gc周期, 每次进行RCU回收和epoch增长
    const size_t mvcc_scan_tick_ = 10;  // 每隔多少个周期执行一次mvcc扫描

    MvccTree default_tree_;
    MvccTree txn_tree_;

    std::atomic<bool> gc_running_ = {true};
    std::mutex gc_mutex_;
    std::condition_variable gc_cond_;
    uint64_t gc_tick_counter_ = 0;
    std::thread gc_thread_;
};


}
}
}

