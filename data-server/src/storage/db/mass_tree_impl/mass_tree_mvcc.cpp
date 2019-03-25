#include "mass_tree_mvcc.h"

#include "storage/db/memdb_batch.h"
#include "storage/db/multi_v_key.h"

#include "iterator.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

MvccMassTree::MvccMassTree() :
        default_tree_(new MassTreeDB),
        txn_tree_(new MassTreeDB) {
}

MvccMassTree::~MvccMassTree() {
    delete default_tree_;
    delete txn_tree_;
}

Status MvccMassTree::get(MassTreeDB* tree, const std::string& key, std::string* value) {
    auto ver = mvcc_.load();
    MultiVersionKey multi_key(key, ver, true);

    Status ret(Status::kNotFound);
    auto iter = tree->NewScaner(multi_key.to_string(), "");
    if (iter->Valid()) {
        multi_key.from_string(iter->Key());
        if (multi_key.key() == key && !multi_key.is_del()) {
            *value = iter->Value();
            ret = Status::OK();
        }
    }
    mvcc_.erase(ver);
    return ret;
}

Status MvccMassTree::Get(const std::string& key, std::string* value) {
    return get(default_tree_, key, value);
}

Status MvccMassTree::Get(void* column_family, const std::string& key, std::string* value) {
    return get(static_cast<MassTreeDB*>(column_family), key, value);
}


Status MvccMassTree::put(MassTreeDB* tree, const std::string& key, const std::string& value) {
    auto ver = mvcc_.insert();
    MultiVersionKey multi_key(key, ver, false);
    tree->Put(multi_key.to_string(), value);
    mvcc_.erase(ver);
    return Status(Status::OK());
}

Status MvccMassTree::Put(const std::string& key, const std::string& value) {
    return put(default_tree_, key, value);
}

Status MvccMassTree::Put(void* column_family, const std::string& key, const std::string& value) {
    return put(static_cast<MassTreeDB *>(column_family), key, value);
}

std::unique_ptr<WriteBatchInterface> MvccMassTree::NewBatch() {
    return std::unique_ptr<WriteBatchInterface>(new MemDBWriteBatch);
}

Status MvccMassTree::Write(WriteBatchInterface* batch) {
    auto mem_batch = dynamic_cast<MemDBWriteBatch*>(batch);
    if (mem_batch != nullptr) {
        return mem_batch->WriteTo(this);
    } else {
        // TODO: type info
        return Status(Status::kInvalidArgument, "batch class", "");
    }
}

Status MvccMassTree::del(MassTreeDB* tree, const std::string& key) {
    auto ver = mvcc_.insert();
    MultiVersionKey multi_key(key, ver, true);
    tree->Put(multi_key.to_string(), "");
    mvcc_.erase(ver);
    return Status(Status::OK());
}

Status MvccMassTree::Delete(const std::string& key) {
    return del(default_tree_, key);
}

Status MvccMassTree::Delete(void* column_family, const std::string& key) {
    return del(static_cast<MassTreeDB*>(column_family), key);
}

Status MvccMassTree::DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) {
    // TODO: delete string*
    return Status(Status::kNotSupported);
}

void* MvccMassTree::DefaultColumnFamily() {
    return default_tree_;
}

void* MvccMassTree::TxnCFHandle() {
    return txn_tree_;
}

IteratorInterface* MvccMassTree::newIter(MassTreeDB* tree, const std::string& start, const std::string& limit) {
    auto version = mvcc_.insert();
    MultiVersionKey start_key(start, version, true);
    MultiVersionKey end_key(limit, std::numeric_limits<uint64_t>::max(), true);
    auto scaner = tree->NewScaner(start_key.to_string(), end_key.to_string());
    return new MassTreeIterator(std::move(scaner), version, [this, version]{ mvcc_.erase(version); });
}

IteratorInterface* MvccMassTree::NewIterator(const std::string& start, const std::string& limit) {
    return newIter(default_tree_, start, limit);
}

Status MvccMassTree::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                                  std::unique_ptr<IteratorInterface>& txn_iter,
                                  const std::string& start, const std::string& limit) {
    data_iter.reset(newIter(default_tree_, start, limit));
    txn_iter.reset(newIter(txn_tree_, start, limit));
    return Status::OK();
}

void MvccMassTree::GetProperty(const std::string& k, std::string* v) {
}

Status MvccMassTree::SetOptions(void* column_family,
                                const std::unordered_map<std::string, std::string>& new_options) {
    return Status::OK();
}

Status MvccMassTree::SetDBOptions(const std::unordered_map<std::string, std::string>& new_options) {
    return Status::OK();
}

void MvccMassTree::PrintMetric() {}

void MvccMassTree::Scrub() {
    uint64_t ver = mvcc_.min_ver();

    auto iter = default_tree_->NewScaner(",", "");
    if (!iter->Valid()) {
        return;
    }

    auto cmp_base_str = iter->Key();
    MultiVersionKey cmp_base;
    cmp_base.from_string(cmp_base_str);

    MultiVersionKey cur_key;
    for (iter->Next(); iter->Valid(); iter->Next()) {
        cur_key.from_string(iter->Key());
        if (cur_key.key() == cmp_base.key()) {
            if (cur_key.ver() < ver) {
                default_tree_->Delete(iter->Key());
            }
        } else {
            if (cmp_base.is_del() && cmp_base.ver() < ver) {
                default_tree_->Delete(iter->Key());
            }
            cmp_base_str = iter->Key();
            cmp_base.from_string(cmp_base_str);
        }
    }
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
