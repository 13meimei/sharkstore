#include "bwtree_db_impl.h"

#include "storage/db/memdb_batch.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

BwTreeDBImpl::BwTreeDBImpl() :
    default_tree_(new BwTreeType),
    txn_tree_(new BwTreeType) {
}

BwTreeDBImpl::~BwTreeDBImpl() {
    delete default_tree_;
    delete txn_tree_;
}

Status BwTreeDBImpl::get(BwTreeType* tree, const std::string& key, std::string* value) {
    std::vector<std::string> value_list;
    tree->GetValue(key, value_list);
    if (value_list.empty()) {
        return Status(Status::kNotFound);
    } else if (value_list.size() == 1) {
        *value = std::move(value_list[0]);
        return Status::OK();
    } else {
        return Status(Status::kInvalidArgument, "get value list size", std::to_string(value_list.size()));
    }
}

Status BwTreeDBImpl::Get(const std::string& key, std::string* value) {
    return get(default_tree_, key, value);
}

Status BwTreeDBImpl::Get(void* column_family, const std::string& key, std::string* value) {
    return get(static_cast<BwTreeType*>(column_family), key, value);
}


Status BwTreeDBImpl::put(BwTreeType* tree, const std::string& key, const std::string& value) {
    while (!tree->Insert(key, value)) {
        tree->Delete(key, "");
    }
    return Status(Status::OK());
}

Status BwTreeDBImpl::Put(const std::string& key, const std::string& value) {
    return put(default_tree_, key, value);
}

Status BwTreeDBImpl::Put(void* column_family, const std::string& key, const std::string& value) {
    return put(static_cast<BwTreeType *>(column_family), key, value);
}

std::unique_ptr<WriteBatchInterface> BwTreeDBImpl::NewBatch() {
    return std::unique_ptr<WriteBatchInterface>(new MemDBWriteBatch);
}

Status BwTreeDBImpl::Write(WriteBatchInterface* batch) {
    auto mem_batch = dynamic_cast<MemDBWriteBatch*>(batch);
    if (mem_batch != nullptr) {
        return mem_batch->WriteTo(this);
    } else {
        // TODO: type info
        return Status(Status::kInvalidArgument, "batch class", "");
    }
}


Status BwTreeDBImpl::del(BwTreeType* tree, const std::string& key) {
    tree->Delete(key, "");
    return Status::OK();
}

Status BwTreeDBImpl::Delete(const std::string& key) {
    return del(default_tree_, key);
}

Status BwTreeDBImpl::Delete(void* column_family, const std::string& key) {
    return del(static_cast<BwTreeType *>(column_family), key);
}

Status BwTreeDBImpl::DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) {
    return Status(Status::kNotSupported);
}

void* BwTreeDBImpl::DefaultColumnFamily() {
    return default_tree_;
}

void* BwTreeDBImpl::TxnCFHandle() {
    return txn_tree_;
}

IteratorInterface* BwTreeDBImpl::NewIterator(const std::string& start, const std::string& limit) {
    return nullptr;
}

Status BwTreeDBImpl::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                    std::unique_ptr<IteratorInterface>& txn_iter,
                    const std::string& start, const std::string& limit) {
    return Status(Status::kNotSupported);
}


void BwTreeDBImpl::GetProperty(const std::string& k, std::string* v) {
}

Status BwTreeDBImpl::SetOptions(void* column_family,
        const std::unordered_map<std::string, std::string>& new_options) {
    return Status::OK();
}

Status BwTreeDBImpl::SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) {
    return Status::OK();
}

void BwTreeDBImpl::PrintMetric() {}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
