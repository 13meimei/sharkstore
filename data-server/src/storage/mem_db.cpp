#include "mem_db.h"
#include <common/ds_config.h>

#include "aggregate_calc.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_encoding.h"
#include "field_value.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "proto/gen/redispb.pb.h"
#include "row_fetcher.h"

namespace sharkstore {

namespace dataserver {
namespace storage {

static const size_t kDefaultMaxSelectLimit = 10000;

static Status updateRow(kvrpcpb::KvPair* row, const RowResult& r);

MemStore::MemStore(const metapb::Range& meta, memstore::Store<std::string>* db):
    table_id_(meta.table_id()),
    range_id_(meta.id()),
    start_key_(meta.start_key()),
    end_key_(meta.end_key()) {
    db_ = db;
    assert(!start_key_.empty());
    assert(!end_key_.empty());
    assert(meta.primary_keys_size() > 0);
    for (int i = 0; i < meta.primary_keys_size(); ++i) {
        primary_keys_.push_back(meta.primary_keys(i));
    }
}

MemStore::~MemStore() {}

Status MemStore::Get(const std::string& key, std::string* value) {
    return Status::OK();
}

Status MemStore::Put(const std::string& key, const std::string& value) {
    return Status::OK();
}

Status MemStore::Delete(const std::string& key) {
    return Status::OK();
}

Status MemStore::Insert(const kvrpcpb::InsertRequest& req, uint64_t* affected) {
    return Status::OK();
}

Status MemStore::Update(const kvrpcpb::UpdateRequest& req, uint64_t* affected, uint64_t* update_bytes) {
    return Status::OK();
}

static void addRow(const kvrpcpb::SelectRequest& req,
                   kvrpcpb::SelectResponse* resp, const RowResult& r) {
    std::string buf;
    for (int i = 0; i < req.field_list_size(); i++) {
        const auto& f = req.field_list(i);
        if (f.has_column()) {
            FieldValue* v = r.GetField(f.column().id());
            EncodeFieldValue(&buf, v);
        }
    }
    auto row = resp->add_rows();
    row->set_key(r.Key());
    row->set_fields(buf);
}

static Status updateRow(kvrpcpb::KvPair* row, const RowResult& r) {
    std::string final_encode_value;
    const auto& origin_encode_value = r.Value();

    for (auto it = r.FieldValueList().begin(); it != r.FieldValueList().end(); it++) {
        auto& field = *it;

        std::string value;
        auto it_field_update = r.UpdateFieldMap().find(field.column_id_);
        if (it_field_update == r.UpdateFieldMap().end()) {
            value.assign(origin_encode_value, field.offset_, field.length_);
            final_encode_value.append(value);
            continue;
        }

        // 更新列值
        // delta field value
        auto it_value_delta = r.UpdateFieldDeltaMap().find(field.column_id_);
        if (it_value_delta == r.UpdateFieldDeltaMap().end()) {
            return Status(Status::kUnknown, std::string("no such update column id: " + field.column_id_), "");
        }
        FieldValue* value_delta = it_value_delta->second;

        // orig field value
        FieldValue* value_orig = r.GetField(field.column_id_);
        if (value_orig == nullptr) {
            return Status(Status::kUnknown, std::string("no such column id " + field.column_id_), "");
        }

        // kv rpc field
        kvrpcpb::Field* field_delta = it_field_update->second;

        switch (field_delta->field_type()) {
            case kvrpcpb::Assign:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Plus:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_orig->Int() + value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_orig->UInt() + value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_orig->Float() + value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Minus:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_orig->Int() - value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_orig->UInt() - value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_orig->Float() - value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Mult:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        value_orig->AssignInt(value_orig->Int() * value_delta->Int());
                        break;
                    case FieldType::kUInt:
                        value_orig->AssignUint(value_orig->UInt() * value_delta->UInt());
                        break;
                    case FieldType::kFloat:
                        value_orig->AssignFloat(value_orig->Float() * value_delta->Float());
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            case kvrpcpb::Div:
                switch (value_delta->Type()) {
                    case FieldType::kInt:
                        if (value_delta->Int() != 0) { value_orig->AssignInt(value_orig->Int() / value_delta->Int()); }
                        break;
                    case FieldType::kUInt:
                        if (value_delta->UInt() != 0) { value_orig->AssignUint(value_orig->UInt() / value_delta->UInt()); }
                        break;
                    case FieldType::kFloat:
                        if (value_delta->Float() != 0) { value_orig->AssignFloat(value_orig->Float() / value_delta->Float()); }
                        break;
                    case FieldType::kBytes:
                        value_orig->AssignBytes(new std::string(value_delta->Bytes()));
                        break;
                }
                break;
            default:
                return Status(Status::kUnknown, "unknown field operator type", "");
        }

        // 重新编码修改后的field value
        EncodeFieldValue(&value, value_orig, field.column_id_);
        final_encode_value.append(value);
    }

    row->set_key(r.Key());
    row->set_value(final_encode_value);

    return Status::OK();
}

Status MemStore::selectSimple(const kvrpcpb::SelectRequest& req,
                           kvrpcpb::SelectResponse* resp) {
    return Status::OK();
}

Status MemStore::selectAggre(const kvrpcpb::SelectRequest& req,
                          kvrpcpb::SelectResponse* resp) {
    return Status::OK();
}

Status MemStore::Select(const kvrpcpb::SelectRequest& req,
                     kvrpcpb::SelectResponse* resp) {
    return Status::OK();
}

Status MemStore::DeleteRows(const kvrpcpb::DeleteRequest& req,
                         uint64_t* affected) {
    return Status::OK();
}

Status MemStore::Truncate() {
    return Status::OK();
}

void MemStore::SetEndKey(std::string end_key) {
    std::unique_lock<std::mutex> lock(key_lock_);
    assert(start_key_ < end_key);
    end_key_ = std::move(end_key);
}

std::string MemStore::GetEndKey() const {
    std::unique_lock<std::mutex> lock(key_lock_);
    return end_key_;
}

Iterator* MemStore::NewIterator(const kvrpcpb::Scope& scope) {
    return nullptr;
}

Iterator* MemStore::NewIterator(std::string start, std::string limit) {
    return nullptr;
}

Status MemStore::BatchDelete(const std::vector<std::string>& keys) {
    return Status::OK();
}

bool MemStore::KeyExists(const std::string& key) {
    return false;
}

Status MemStore::BatchSet(
    const std::vector<std::pair<std::string, std::string>>& keyValues) {
    return Status::OK();
}

Status MemStore::RangeDelete(const std::string& start, const std::string& limit) {
    return Status::OK();
}

Status MemStore::ApplySnapshot(const std::vector<std::string>& datas) {
    return Status::OK();
}

void MemStore::addMetricRead(uint64_t keys, uint64_t bytes) {
    metric_.AddRead(keys, bytes);
    g_metric.AddRead(keys, bytes);
}

void MemStore::addMetricWrite(uint64_t keys, uint64_t bytes) {
    metric_.AddWrite(keys, bytes);
    g_metric.AddWrite(keys, bytes);
}

Status MemStore::parseSplitKey(const std::string& key, range::SplitKeyMode mode, std::string *split_key) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient key size", EncodeToHex(key));
    }

    switch (mode) {
        case range::SplitKeyMode::kNormal:
            *split_key = key;
            return Status::OK();
        case range::SplitKeyMode::kRedis: {
            size_t offset = kRowPrefixLength;
            // decode ns
            if (!DecodeVarintAscending(key, offset, nullptr)) {
                return Status(Status::kCorruption, "decode redis value ns", EncodeToHex(key));
            }
            // decode real key
            std::string realKey;
            if (!DecodeBytesAscending(key, offset, &realKey)) {
                return Status(Status::kCorruption, "decode redis value real key", EncodeToHex(key));
            }
            assert(offset <= key.size());
            split_key->assign(key.c_str(), offset);
            return Status::OK();
        }
        case range::SplitKeyMode::kLockWatch:{
            size_t offset = kRowPrefixLength;
            std::string watch_key;
            if (!DecodeBytesAscending(key, offset, &watch_key)) {
                return Status(Status::kCorruption, "decode watch key", EncodeToHex(key));
            }
            assert(offset <= key.size());
            split_key->assign(key.c_str(), offset);
            return Status::OK();
        }
        default:
            return Status(Status::kNotSupported, "split key mode",
                    std::to_string(static_cast<int>(mode)));
    }
}

Status MemStore::StatSize(uint64_t split_size, range::SplitKeyMode mode,
                  uint64_t *real_size, std::string *split_key) {
    uint64_t total_size = 0;

    // The number of the same characters is greater than
    // the length of start_key_ and more than 5,
    // then the length of the split_key is
    // start_key_.length() + 5
    auto max_len = start_key_.length() + 5;

    std::unique_ptr<Iterator> it(NewIterator());
    std::string middle_key;
    std::string first_key;

    while (it->Valid()) {
        if (mode != range::SplitKeyMode::kNormal && first_key.empty()) {
            auto s = parseSplitKey(it->key(), mode, &first_key);
            if (!s.ok()) return s;
            assert(!first_key.empty());
        }

        total_size += it->key_size();
        total_size += it->value_size();

        if (total_size >= split_size) {
            middle_key = it->key();
            it->Next();
            break;
        }

        it->Next();
    }

    if (!it->Valid()) {
        if (!it->status().ok()) {
            return it->status();
        } else {
            return Status(Status::kUnexpected, "no more data", std::to_string(total_size));
        }
    }

    // 特殊模式（非Normal）split key会是原始key的前缀，
    // 如果first key跟split key相等， 则需要查找下一个前缀key作为split key,
    // 以防止 [start_key, split_key) 区间内的数据为空
    bool find_next = false;
    if (mode == range::SplitKeyMode::kNormal) {
        *split_key = SliceSeparate(it->key(), middle_key, max_len);
    } else {
        auto s = parseSplitKey(middle_key, mode, split_key);
        if (!s.ok()) {
            return s;
        }
        // 相等，需要找下一个
        find_next = (first_key == *split_key);
    }

    // 遍历剩下一的一半
    while (it->Valid()) {
        total_size += it->key_size();
        total_size += it->value_size();

        if (find_next) {
            assert(mode != range::SplitKeyMode::kNormal);
            auto s = parseSplitKey(it->key(), mode, split_key);
            if (!s.ok()) return s;
            if (*split_key != first_key) { // 遇到不一样的key
                find_next = false;
            }
        }

        it->Next();
    }
    if (!it->status().ok()) {
        return it->status();
    }

    if (find_next) {
        return Status(Status::kNotFound, "next split key", EncodeToHex(*split_key));
    }

    *real_size = total_size;
    return Status::OK();
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
