#include "select_txn.h"

#include "base/util.h"
#include "common/ds_encoding.h"
#include "util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

TxnRowValue::~TxnRowValue() {
    std::for_each(fields_.begin(), fields_.end(),
            [](std::map<uint64_t, FieldValue*>::value_type& p) { delete p.second; });
}

bool TxnRowValue::AddField(uint64_t col, std::unique_ptr<FieldValue>& field) {
    auto ret = fields_.emplace(col, field.get()).second;
    if (ret) {
        auto p = field.release();
        (void)p;
    }
    return ret;
}

FieldValue* TxnRowValue::GetField(uint64_t col) const {
    auto it = fields_.find(col);
    if (it != fields_.cend()) {
        return it->second;
    } else {
        return nullptr;
    }
}

void TxnRowValue::Encode(const txnpb::SelectRequest& req, txnpb::RowValue* to) {
    std::string buf;
    for (const auto& field: req.field_list()) {
        if (field.has_column()) {
            auto fv = GetField(field.column().id());
            EncodeFieldValue(&buf, fv);
        }
    }
    to->set_fields(buf);
    to->set_version(version_);
}

TxnRowDecoder::TxnRowDecoder(const std::vector<metapb::Column>& primary_keys,
        const txnpb::SelectRequest& req) : primary_keys_(primary_keys) {
    for (const auto& field: req.field_list()) {
        if (field.has_column()) {
            cols_.emplace(field.column().id(), field.column());
        }
    }
    for (const auto& match: req.where_filters()) {
        cols_.emplace(match.column().id(), match.column());
        filters_.push_back(match);
    }
}

Status TxnRowDecoder::DecodeAndFilter(const std::string& key, const std::string& buf,
                       TxnRowValue& row, bool& matched) {
    auto s = decodePrimaryKeys(key, row);
    if (!s.ok()) {
        return s;
    }
    s = decodeFields(buf, row);
    if (!s.ok()) {
        return s;
    }

    matched = true;
    if (filters_.empty()) {
        return Status::OK();
    } else {
        return matchRow(row, filters_, matched);
    }
}

Status TxnRowDecoder::decodePrimaryKeys(const std::string& key, TxnRowValue& row) {
    if (key.size() <= kRowPrefixLength) {
        return Status(Status::kCorruption, "insufficient row key length", EncodeToHex(key));
    }
    size_t offset = kRowPrefixLength;
    assert(!primary_keys_.empty());
    Status s;
    for (const auto& column: primary_keys_) {
        std::unique_ptr<FieldValue> field;
        auto it = cols_.find(column.id());
        if (it != cols_.end()) {
            s = decodePK(key, offset, column, &field);
        } else {
            s = decodePK(key, offset, column, nullptr);
        }
        if (!s.ok()) {
            return s;
        }
        if (field != nullptr) {
            if (!row.AddField(column.id(), field)) {
                return Status(Status::kDuplicate, "repeated field on column", column.name());
            }
        }
    }
    return Status::OK();
}

Status TxnRowDecoder::decodeFields(const std::string& buf, TxnRowValue& row) {
    uint32_t col_id = 0;
    EncodeType enc_type;
    size_t tag_offset = 0;
    for (size_t offset = 0; offset < buf.size();) {
        // 解析列ID
        tag_offset = offset;
        if (!DecodeValueTag(buf, tag_offset, &col_id, &enc_type)) {
            return Status(Status::kCorruption,
                          std::string("decode row value tag failed at offset ") + std::to_string(offset),
                          EncodeToHexString(buf));
        }

        // 解码version列
        if (col_id == kVersionColumnID) {
            int64_t version = 0;
            if (!DecodeIntValue(buf, offset, &version)) {
                return Status(Status::kCorruption,
                              std::string("decode int value failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            } else {
                row.SetVersion(static_cast<uint64_t>(version));
            }
            continue;
        }

        // 检查该列ID对应的列是否需要Decode
        auto it = cols_.find(col_id);
        if (it == cols_.end()) {
            if (!SkipValue(buf, offset)) {
                return Status(Status::kCorruption,
                              std::string("decode skip value tag failed at offset ") + std::to_string(offset),
                              EncodeToHexString(buf));
            }
            continue;
        }

        // 解码列值
        std::unique_ptr<FieldValue> value;
        auto s = decodeField(buf, offset, it->second, value);
        if (!s.ok()) {
            return s;
        }
        if (!row.AddField(it->first, value)) {
            return Status(Status::kDuplicate, "repeated field on column", it->second.name());
        }
    }
    return Status::OK();
}


TxnRowFetcher::TxnRowFetcher(Store& s, const txnpb::SelectRequest& req):
    store_(s),
    req_(req),
    decoder_(s.GetPrimaryKeys(), req) {
}

Status TxnRowFetcher::addRow(const std::string& key, const std::string& buf, txnpb::Row& row) {
    TxnRowValue value;
    bool matched = false;
    auto s = decoder_.DecodeAndFilter(key, buf, value, matched);
    if (!s.ok()) {
        return s;
    }
    if (!matched) {
        return Status::OK();
    }
    value.Encode(req_, row.mutable_value());
    return Status::OK();
}

Status TxnRowFetcher::addIntent(const txnpb::TxnValue &txn_value, txnpb::Row &row) {
    if (txn_value.intent().typ() == txnpb::INSERT) {
        TxnRowValue value;
        bool matched = false;
        auto s = decoder_.DecodeAndFilter(txn_value.intent().key(), txn_value.intent().value(), value, matched);
        if (!s.ok()) {
            return s;
        }
        if (!matched) {
            return Status::OK();
        }
        value.Encode(req_, row.mutable_intent()->mutable_value());
    }
    auto row_intent = row.mutable_intent();
    row_intent->set_txn_id(txn_value.txn_id());
    row_intent->set_op_type(txn_value.intent().typ());
    const auto& primary = txn_value.intent().is_primary() ? txn_value.intent().key(): txn_value.primary_key();
    row_intent->set_primary_key(primary);
    row_intent->set_timeout(isExpired(txn_value.expired_at()));
    return Status::OK();
}

std::unique_ptr<TxnRowFetcher> NewTxnRowFetcher(Store& s, const txnpb::SelectRequest& req) {
    if (!req.key().empty()) {
        return std::unique_ptr<TxnRowFetcher>(new PointRowFetcher(s, req));
    } else {
        return std::unique_ptr<TxnRowFetcher>(new RangeRowFetcher(s, req));
    }
}

/// PointRowFetcher
PointRowFetcher::PointRowFetcher(Store& s, const txnpb::SelectRequest& req) :
    TxnRowFetcher(s, req) {
    assert(!req.key().empty());
}

Status PointRowFetcher::Next(txnpb::Row& row, bool& over) {
    if (fetched_) { // only once
        over = true;
        return Status::OK();
    }

    fetched_ = true;

    auto s = getFromData(row);
    if (!s.ok()) {
        return s;
    }
    s = getFromIntent(row);
    if (!s.ok()) {
        return s;
    }
    if (row.has_value() || row.has_intent()) { // got row or intent
        row.set_key(req_.key());
    } else { // get nothing
        over = true;
    }
    return Status::OK();
}

Status PointRowFetcher::getFromData(txnpb::Row& row) {
    std::string buf;
    auto s = store_.Get(req_.key(), &buf);
    if (s.ok()) {
        return addRow(req_.key(), buf, row);
    } else if (s.code() == Status::kNotFound) {
        return Status::OK();
    } else {
        return s;
    }
}

Status PointRowFetcher::getFromIntent(txnpb::Row& row) {
    txnpb::TxnValue txn_value;
    auto s = store_.GetTxnValue(req_.key(), &txn_value);
    if (s.ok()) {
        return addIntent(txn_value, row);
    } else if (s.code() == Status::kNotFound) {
        return Status::OK();
    } else {
        return s;
    }
}


/// RangeRowFetcher
RangeRowFetcher::RangeRowFetcher(Store& s, const txnpb::SelectRequest& req) :
    TxnRowFetcher(s, req) {
}

RangeRowFetcher::~RangeRowFetcher() {
    delete data_iter_;
    delete txn_iter_;
}

Status RangeRowFetcher::Next(txnpb::Row& row, bool& over) {
    return Status(Status::kNotSupported);
}

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
