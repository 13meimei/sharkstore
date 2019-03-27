_Pragma("once");

#include "storage/db/db_interface.h"
#include "storage/db/mvcc.h"
#include "mass_tree_db.h"

class MassTreeTest;

namespace sharkstore {
namespace dataserver {
namespace storage {

class MvccMassTree : public DbInterface {
public:
    friend class MassTreeIterator;
    friend class ::MassTreeTest;

    MvccMassTree();
    ~MvccMassTree();

    bool IsInMemory() override() { return true; }

    Status Open() override { return Status::OK(); }

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

    void PrintMetric() override;

    void Scrub() override;

public:
    MassTreeDB *GetDefaultTree() { return default_tree_; }

    MassTreeDB *GetTxnTree() { return txn_tree_; }

private:
    Status get(MassTreeDB *tree, const std::string &key, std::string *value);
    Status put(MassTreeDB *tree, const std::string &key, const std::string &value);
    Status del(MassTreeDB *tree, const std::string &key);
    Status deleteRange(MassTreeDB *tree, const std::string& begin_key, const std::string& end_key);
    IteratorInterface *newIter(MassTreeDB *tree, const std::string &start, const std::string &limit);

private:
    MassTreeDB *default_tree_ = nullptr;
    MassTreeDB *txn_tree_ = nullptr;
    Mvcc mvcc_;
};


}
}
}

