#include "mass_tree_mvcc.h"

#include "storage/db/memdb_batch.h"
#include "storage/db/multi_v_key.h"

#include "iterator.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Status MvccMassTree::get(MvccTree *family, const std::string& key, std::string* value) {
    auto ver = family->mvcc.insert();
    MultiVersionKey multi_key(key, ver, true);

    Status ret(Status::kNotFound);
    auto iter = family->tree.NewScaner(multi_key.to_string(), "", 1);
    if (iter->Valid()) {
        multi_key.from_string(iter->Key());
        if (multi_key.key() == key && !multi_key.is_del()) {
            *value = iter->Value();
            ret = Status::OK();
        }
    }
    family->mvcc.erase(ver);
    return ret;
}

Status MvccMassTree::Get(const std::string& key, std::string* value) {
    return get(&default_tree_, key, value);
}

Status MvccMassTree::Get(void* column_family, const std::string& key, std::string* value) {
    return get(static_cast<MvccTree*>(column_family), key, value);
}


Status MvccMassTree::put(MvccTree* family, const std::string& key, const std::string& value) {
    auto ver = family->mvcc.insert();
    MultiVersionKey multi_key(key, ver, false);
    family->tree.Put(multi_key.to_string(), value);
    family->mvcc.erase(ver);
    return Status(Status::OK());
}

Status MvccMassTree::Put(const std::string& key, const std::string& value) {
    return put(&default_tree_, key, value);
}

Status MvccMassTree::Put(void* column_family, const std::string& key, const std::string& value) {
    return put(static_cast<MvccTree*>(column_family), key, value);
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

Status MvccMassTree::del(MvccTree *family, const std::string& key) {
    auto ver = family->mvcc.insert();
    MultiVersionKey multi_key(key, ver, true);
    family->tree.Put(multi_key.to_string(), "");
    family->mvcc.erase(ver);
    return Status(Status::OK());
}

Status MvccMassTree::Delete(const std::string& key) {
    return del(&default_tree_, key);
}

Status MvccMassTree::Delete(void* column_family, const std::string& key) {
    return del(static_cast<MvccTree*>(column_family), key);
}

IteratorInterface* MvccMassTree::newIter(MvccTree* family, const std::string& start, const std::string& limit) {
    auto version = family->mvcc.insert();
    MultiVersionKey start_key(start, version, true);
    MultiVersionKey end_key(limit, std::numeric_limits<uint64_t>::max(), true);
    auto scaner = family->tree.NewScaner(start_key.to_string(), limit.empty() ? "" : end_key.to_string());
    return new MassTreeIterator(std::move(scaner), version, [this, version, family]{ family->mvcc.erase(version); });
}

Status MvccMassTree::deleteRange(MvccTree *family, const std::string& begin_key, const std::string& end_key) {
    std::unique_ptr<IteratorInterface> iter(newIter(family, begin_key, end_key));
    auto version = family->mvcc.insert();
    while (iter->Valid()) {
        MultiVersionKey multi_key(iter->key(), version, true);
        family->tree.Put(multi_key.to_string(), "");
        iter->Next();
    }
    family->mvcc.erase(version);
    return Status(Status::OK());
}

Status MvccMassTree::DeleteRange(void* column_family, const std::string& begin_key, const std::string& end_key) {
    return deleteRange(static_cast<MvccTree*>(column_family), begin_key, end_key);
}

void* MvccMassTree::DefaultColumnFamily() {
    return &default_tree_;
}

void* MvccMassTree::TxnCFHandle() {
    return &txn_tree_;
}

IteratorInterface* MvccMassTree::NewIterator(const std::string& start, const std::string& limit) {
    return newIter(&default_tree_, start, limit);
}

Status MvccMassTree::NewIterators(std::unique_ptr<IteratorInterface>& data_iter,
                                  std::unique_ptr<IteratorInterface>& txn_iter,
                                  const std::string& start, const std::string& limit) {
    data_iter.reset(newIter(&default_tree_, start, limit));
    txn_iter.reset(newIter(&txn_tree_, start, limit));
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

void MvccMassTree::Scrub(MvccTree *family) {
    uint64_t ver = family->mvcc.min_ver();

    auto iter = family->tree.NewScaner("", "");
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
                family->tree.Delete(iter->Key());
            }
        } else {
            if (cmp_base.is_del() && cmp_base.ver() < ver) {
                family->tree.Delete(cmp_base_str);
            }
            cmp_base_str = iter->Key();
            cmp_base.from_string(cmp_base_str);
        }
    }

    if (cmp_base.is_del() && cmp_base.ver() < ver) {
        family->tree.Delete(cmp_base_str);
    }
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
