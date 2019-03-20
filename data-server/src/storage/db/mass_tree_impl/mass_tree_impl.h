_Pragma("once");

#include "storage/db/db_interface.h"

#include "masstree-beta/config.h"
#include "masstree-beta/masstree.hh"
#include "masstree-beta/timestamp.hh"
#include "masstree-beta/kvthread.hh"

namespace sharkstore {
namespace dataserver {
namespace storage {

struct ThreadInfoDeleter {
    void operator()(threadinfo*) const {}
};

class StringValuePrinter {
public:
    static void print(std::string *value, FILE* f, const char* prefix,
                      int indent, Masstree::Str key, kvtimestamp_t,
                      char* suffix) {
        fprintf(f, "%s%*s%.*s = %s%s\n",
                prefix, indent, "", key.len, key.s, (value ? value->c_str() : ""), suffix);
    }
};

struct default_query_table_params : public Masstree::nodeparams<15, 15> {
    typedef std::string* value_type;
    typedef StringValuePrinter value_print_type;
    typedef ::threadinfo threadinfo_type;
};

using TreeType = Masstree::basic_table<default_query_table_params>;

class MassTreeDBImpl : public DbInterface {
public:
    MassTreeDBImpl();
    ~MassTreeDBImpl();

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

public:
    TreeType* GetDefaultTree() { return default_tree_; }
    TreeType* GetTxnTree() { return txn_tree_; }
    std::unique_ptr<threadinfo, ThreadInfoDeleter>& GetThreadInfo() { return thread_info_; }

private:
    thread_local static std::unique_ptr<threadinfo, ThreadInfoDeleter> thread_info_;

private:
    static Status get(TreeType* tree, const std::string& key, std::string* value);
    static Status put(TreeType* tree, const std::string& key, const std::string& value);
    static Status del(TreeType* tree, const std::string& key);

private:
    TreeType* default_tree_ = nullptr;
    TreeType* txn_tree_ = nullptr;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */




