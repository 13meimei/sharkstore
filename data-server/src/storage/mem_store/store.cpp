#include "store.h"
#include "iterator.h"
#include <common/ds_config.h>

#include "storage/aggregate_calc.h"
#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_encoding.h"
#include "storage/field_value.h"
#include "proto/gen/raft_cmdpb.pb.h"
#include "proto/gen/redispb.pb.h"
#include "storage/row_fetcher.h"
#include "frame/sf_logger.h"

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
    FLOG_DEBUG("mem store get: key(%s) key_len(%d)",
               key.c_str(), key.length());

    auto ret = db_->Get(key, value);
    if (ret != 0) {
        return Status(Status::kIOError);
    }
    addMetricRead(1, key.size() + value->size());
    return Status::OK();
}

Status MemStore::Put(const std::string& key, const std::string& value) {
    FLOG_DEBUG("mem store put: key(%s) key_len(%d) value(%s) value len(%v)",
               key.c_str(), key.length(), value.c_str(), value.length());

    auto ret = db_->Put(key, value);
    if (ret != 0) {
        return Status(Status::kIOError);
    }
    addMetricWrite(1, key.size() + value.size());
    return Status::OK();
}

Status MemStore::Delete(const std::string& key) {
    db_->Delete(key);
    return Status::OK();
}

Status MemStore::Insert(const kvrpcpb::InsertRequest& req, uint64_t* affected) {
    FLOG_DEBUG("mem store insert...");

    uint64_t bytes_written = 0;
    std::string value;
    bool check_dup = req.check_duplicate();
    *affected = 0;
    for (int i = 0; i < req.rows_size(); ++i) {
        const kvrpcpb::KeyValue& kv = req.rows(i);
        auto s = db_->Put(kv.key(), kv.value());
        if (s != 0) {
            FLOG_ERROR("mem store put key(%s) failed", kv.key().c_str());
            return Status(Status::kIOError);
        }
        *affected = *affected + 1;
        bytes_written += (kv.key().size(), kv.value().size());
    }

    addMetricWrite(*affected, bytes_written);
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
    RowFetcher f(*this, req);
    Status s;
    std::unique_ptr<RowResult> r(new RowResult);
    bool over = false;
    uint64_t count = 0;
    uint64_t all = 0;
    uint64_t limit = req.has_limit() ? req.limit().count() : kDefaultMaxSelectLimit;
    uint64_t offset = req.has_limit() ? req.limit().offset() : 0;
    while (!over && s.ok()) {
        over = false;
        s = f.Next(r.get(), &over);
        if (s.ok() && !over) {
            ++all;
            if (all > offset) {
                addRow(req, resp, *r);
                if (++count >= limit) break;
            }
        }
    }
    resp->set_offset(all);
    return s;
}

Status MemStore::selectAggre(const kvrpcpb::SelectRequest& req,
                          kvrpcpb::SelectResponse* resp) {
    // 暂时不支持带group by的聚合函数
    if (req.group_bys_size() > 0) {
        return Status(Status::kNotSupported, "select",
                      "aggregateion with group by clause");
    }

    std::vector<std::unique_ptr<AggreCalculator>> aggre_cals;
    aggre_cals.reserve(req.field_list_size());
    for (int i = 0; i < req.field_list_size(); ++i) {
        const auto& field = req.field_list(i);
        assert(field.typ() == kvrpcpb::SelectField_Type_AggreFunction);
        // TODO:
        auto cal = AggreCalculator::New(
                field.aggre_func(), field.has_column() ? &field.column() : nullptr);
        if (cal == nullptr) {
            return Status(
                    Status::kNotSupported, "select",
                    std::string("aggregate funtion: ") + field.aggre_func());
        } else {
            aggre_cals.push_back(std::move(cal));
        }
    }

    RowFetcher f(*this, req);
    Status s;
    std::unique_ptr<RowResult> r(new RowResult);
    bool over = false;
    while (!over && s.ok()) {
        over = false;
        s = f.Next(r.get(), &over);
        if (s.ok() && !over) {
            for (size_t i = 0; i < aggre_cals.size(); ++i) {
                const auto& field = req.field_list(i);
                if (field.has_column()) {
                    aggre_cals[i]->Add(r->GetField(field.column().id()));
                } else {
                    aggre_cals[i]->Add(nullptr);
                }
            }
        }
    }
    if (s.ok()) {
        std::string buf;
        auto row = resp->add_rows();
        for (auto& cal : aggre_cals) {
            auto f = cal->Result();
            EncodeFieldValue(&buf, f.get());
            row->add_aggred_counts(cal->Count());
        }
        row->set_fields(buf);
    }
    return s;
}

Status MemStore::Select(const kvrpcpb::SelectRequest& req,
                     kvrpcpb::SelectResponse* resp) {
    if (req.field_list_size() == 0) {
        return Status(Status::kNotSupported, "select",
                      "invalid select field list size");
    }

    bool has_aggre = false, has_column = false;
    for (int i = 0; i < req.field_list_size(); ++i) {
        auto type = req.field_list(i).typ();
        switch (type) {
            case kvrpcpb::SelectField_Type_Column:
                has_column = true;
                break;
            case kvrpcpb::SelectField_Type_AggreFunction:
                has_aggre = true;
                break;
            default:
                return Status(Status::kInvalidArgument, "select",
                              std::string("unknown select field type: ") +
                              kvrpcpb::SelectField_Type_Name(type));
        }
    }
    // 既有聚合函数又有普通的列，暂时不支持
    if (has_aggre && has_column) {
        return Status(Status::kNotSupported, "select",
                      "mixture of aggregate and column select field");
    } else if (has_column) {
        return selectSimple(req, resp);
    } else {
        return selectAggre(req, resp);
    }
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

IteratorInterface* MemStore::NewIterator(const kvrpcpb::Scope& scope) {
    return new MemIterator(db_->NewIterator(scope.start(), scope.limit()),
                           scope.start(), scope.limit());
}

IteratorInterface* MemStore::NewIterator(std::string start, std::string limit) {
    return new MemIterator(db_->NewIterator(start, limit), start, limit);
}

Status MemStore::BatchDelete(const std::vector<std::string>& keys) {
    return Status::OK();
}

bool MemStore::KeyExists(const std::string& key) {
    return false;
}

Status MemStore::BatchSet(const std::vector<std::pair<std::string, std::string>>& keyValues) {
    return Status::OK();
}

Status MemStore::RangeDelete(const std::string& start, const std::string& limit) {
    db_->DeleteRange(start, limit);
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

    std::unique_ptr<IteratorInterface> it(NewIterator());
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
