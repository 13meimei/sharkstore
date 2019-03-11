#include "bwtree_db_impl.h"

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
        tree->Delete(key);
    }
}

Status BwTreeDBImpl::Put(const std::string& key, const std::string& value) {
    return Status(Status::kNotSupported);
}

Status BwTreeDBImpl::Put(void* column_family, const std::string& key, const std::string& value) {
    return Status(Status::kNotSupported);
}

std::unique_ptr<WriteBatchInterface> BwTreeDBImpl::NewBatch() {
    return nullptr;
}

Status BwTreeDBImpl::Write(WriteBatchInterface* batch) {
    return Status(Status::kNotSupported);
}

Status BwTreeDBImpl::Delete(const std::string& key) {
    return Status(Status::kNotSupported);
}

Status BwTreeDBImpl::Delete(void* column_family, const std::string& key) {
    return Status(Status::kNotSupported);
}

Status BwTreeDBImpl::DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) {
    return Status(Status::kNotSupported);
}

void* BwTreeDBImpl::DefaultColumnFamily() {
    return nullptr;
}

void* BwTreeDBImpl::TxnCFHandle() {
    return nullptr;
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
