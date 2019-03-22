#include "mass_tree_impl.h"

#include "storage/db/memdb_batch.h"
#include "storage/db/multi_v_key.h"

#include "masstree-beta/masstree_insert.hh"
#include "masstree-beta/masstree_remove.hh"
#include "iterator.h"

volatile mrcu_epoch_type globalepoch = 1;     // global epoch, updated by main thread regularly
volatile mrcu_epoch_type active_epoch = 1;

namespace sharkstore {
namespace dataserver {
namespace storage {

thread_local std::unique_ptr<threadinfo, ThreadInfoDeleter> MassTreeDBImpl::thread_info_(
        threadinfo::make(threadinfo::TI_PROCESS, -1));

MassTreeDBImpl::MassTreeDBImpl() :
        default_tree_(new TreeType),
        txn_tree_(new TreeType) {
    default_tree_->initialize(*thread_info_);
    txn_tree_->initialize(*thread_info_);
}

MassTreeDBImpl::~MassTreeDBImpl() {
    delete default_tree_;
    delete txn_tree_;
}

Status MassTreeDBImpl::get(TreeType* tree, const std::string& key, std::string* value) {
    Masstree::Str tree_key(key);
    TreeType::value_type tree_value = nullptr;
    if (tree->get(tree_key, tree_value, *thread_info_)) {
        if (tree_value != nullptr) {
            *value = *tree_value;
            return Status::OK();
        } else {
            return Status(Status::kIOError, "mass tree get", "tree value is nullptr");
        }
    } else {
        return Status(Status::kNotFound);
    }
}

Status MassTreeDBImpl::Get(const std::string& key, std::string* value) {
    return get(default_tree_, key, value);
}

Status MassTreeDBImpl::Get(void* column_family, const std::string& key, std::string* value) {
    return get(static_cast<TreeType*>(column_family), key, value);
}


Status MassTreeDBImpl::put(TreeType* tree, const std::string& key, const std::string& value) {
    auto ver = mvcc_.insert();
    MultiVersionKey multi_key(key, ver, true);

    Masstree::Str tree_key(multi_key.to_string());
    TreeType::cursor_type lp(*tree, tree_key);
    if (lp.find_insert(*thread_info_)) {
        // TODO: free old value
    }
    lp.value() = new std::string(value);
    lp.finish(1, *thread_info_);

    mvcc_.erase(ver);
    return Status(Status::OK());
}

Status MassTreeDBImpl::Put(const std::string& key, const std::string& value) {
    return put(default_tree_, key, value);
}

Status MassTreeDBImpl::Put(void* column_family, const std::string& key, const std::string& value) {
    return put(static_cast<TreeType *>(column_family), key, value);
}

std::unique_ptr<WriteBatchInterface> MassTreeDBImpl::NewBatch() {
    return std::unique_ptr<WriteBatchInterface>(new MemDBWriteBatch);
}

Status MassTreeDBImpl::Write(WriteBatchInterface* batch) {
    auto mem_batch = dynamic_cast<MemDBWriteBatch*>(batch);
    if (mem_batch != nullptr) {
        return mem_batch->WriteTo(this);
    } else {
        // TODO: type info
        return Status(Status::kInvalidArgument, "batch class", "");
    }
}

Status MassTreeDBImpl::del(TreeType* tree, const std::string& key) {
    auto ver = mvcc_.insert();
    MultiVersionKey multi_key(key, ver, true);

    Masstree::Str tree_key(multi_key.to_string());
    TreeType::cursor_type lp(*tree, tree_key);
    if (lp.find_insert(*thread_info_)) {
        // TODO: free old value
    }
    lp.value() = new std::string("");
    lp.finish(1, *thread_info_);

    mvcc_.erase(ver);
    return Status(Status::OK());
}

Status MassTreeDBImpl::Delete(const std::string& key) {
    return del(default_tree_, key);
}

Status MassTreeDBImpl::Delete(void* column_family, const std::string& key) {
    return del(static_cast<TreeType *>(column_family), key);
}

Status MassTreeDBImpl::DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) {
    // TODO: delete string*
    return Status(Status::kNotSupported);
}

void* MassTreeDBImpl::DefaultColumnFamily() {
    return default_tree_;
}

void* MassTreeDBImpl::TxnCFHandle() {
    return txn_tree_;
}

IteratorInterface* MassTreeDBImpl::NewIterator(const std::string& start, const std::string& limit) {
    return new MassTreeIterator(default_tree_, start, limit, this);
}

Status MassTreeDBImpl::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                                  std::unique_ptr<IteratorInterface>& txn_iter,
                                  const std::string& start, const std::string& limit) {
    // TODO:
    return Status(Status::kNotSupported);
}


void MassTreeDBImpl::GetProperty(const std::string& k, std::string* v) {
}

Status MassTreeDBImpl::SetOptions(void* column_family,
                                const std::unordered_map<std::string, std::string>& new_options) {
    return Status::OK();
}

Status MassTreeDBImpl::SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) {
    return Status::OK();
}

void MassTreeDBImpl::PrintMetric() {}

void MassTreeDBImpl::Scrub() {
    uint64_t ver = mvcc_.min_ver();

    Scaner iter(default_tree_, "", "");
    if (!iter.Valid()) {
        return;
    }

    auto cmp_base_str = iter.Key();
    MultiVersionKey cmp_base;
    cmp_base.from_string(cmp_base_str);

    MultiVersionKey cur_key;
    for (iter.Next(); iter.Valid(); iter.Next()) {
        cur_key.from_string(iter.Key());
        if (cur_key.key() == cmp_base.key()) {
            if (cur_key.ver() < ver) {
                del(default_tree_, iter.Key());
            }
        } else {
            if (cmp_base.is_del() && cmp_base.ver() < ver) {
                del(default_tree_, cmp_base_str);
            }
            cmp_base_str = iter.Key();
            cmp_base.from_string(cmp_base_str);
        }
    }
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
