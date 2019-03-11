_Pragma("once");

#include "storage/db/db_interface.h"
#include "bwtree.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class BwTreeDBImpl : public DbInterface {
public:
    BwTreeDBImpl();
    ~BwTreeDBImpl();

    Status Open() override { return Status::OK(); }

    Status Get(const std::string& key, std::string* value) override;
    Status Get(void* column_family, const std::string& key, std::string* value) override;
    Status Put(const std::string& key, const std::string& value) override;
    Status Put(void* column_family, const std::string& key, const std::string& value) override;

    std::unique_ptr<WriteBatchInterface> NewBatch() override;
    Status Write(WriteBatchInterface* batch) override;

    Status Delete(const std::string& key) override;
    Status Delete(void* column_family, const std::string& key) override;
    Status DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) override;

    void* DefaultColumnFamily() override;
    void* TxnCFHandle() override;

    IteratorInterface* NewIterator(const std::string& start, const std::string& limit) override;
    Status NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                        std::unique_ptr<IteratorInterface>& txn_iter,
                        const std::string& start, const std::string& limit) override;

    void GetProperty(const std::string& k, std::string* v) override;
    Status SetOptions(void* column_family, const std::unordered_map<std::string, std::string>& new_options) override;
    Status SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) override;
    void PrintMetric() override;

private:
    using BwTreeType = wangziqi2013::bwtree::BwTree<std::string, std::string>;

    static Status get(BwTreeType* tree, const std::string& key, std::string* value);
    static Status put(BwTreeType* tree, const std::string& key, const std::string& value);

private:

    BwTreeType* default_tree_ = nullptr;
    BwTreeType* txn_tree_ = nullptr;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */



