//
// Created by young on 19-2-14.
//

#include "storage/db/skiplist_impl/write_batch.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Status SkipListWriteBatch::Put(const std::string &key, const std::string &value) {
    return Put(nullptr, key, value);
}

Status SkipListWriteBatch::Put(void* column_family, const std::string& key, const std::string& value) {
    auto ent = std::unique_ptr<BatchEntry>(new BatchEntry);
    ent->cf = column_family;
    ent->type = EntryType::kPut;
    ent->key = key;
    ent->value = value;
    entries_.push_back(std::move(ent));
    return Status::OK();
}

Status SkipListWriteBatch::Delete(const std::string &key) {
    return Delete(nullptr, key);
}

Status SkipListWriteBatch::Delete(void* column_family, const std::string& key) {
    auto ent = std::unique_ptr<BatchEntry>(new BatchEntry);
    ent->cf = column_family;
    ent->type = EntryType::kDelete;
    ent->key = key;
    entries_.push_back(std::move(ent));
    return Status::OK();
}

Status SkipListWriteBatch::WriteTo(SkipListDBImpl* db) {
    Status s;
    for (const auto& ent: entries_) {
        if (ent->type == EntryType::kPut) {
            if (ent->cf == nullptr) {
                s = db->Put(ent->key, ent->value);
            } else {
                s = db->Put(ent->cf, ent->key, ent->value);
            }
        } else {
            if (ent->cf == nullptr) {
                s = db->Delete(ent->key);
            } else {
                s = db->Delete(ent->cf, ent->key);
            }
        }
        if (!s.ok()) {
            return s;
        }
    }
    return s;
}

}
}
}