#include "select_txn.h"

#include "row_util.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

TxnRowValue::~TxnRowValue() {
    std::for_each(fields_.begin(), fields_.end(),
            [](std::map<uint64_t, FieldValue*>::value_type& p) { delete p.second; });
}

bool TxnRowValue::AddField(uint64_t col, FieldValue* fval) {
    return fields_.emplace(col, fval).second;
}

FieldValue* TxnRowValue::GetField(uint64_t col) const {
    auto it = fields_.find(col);
    if (it != fields_.cend()) {
        return it->second;
    } else {
        return nullptr;
    }
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
                       TxnRowValue* result, bool* matched) {
    auto s = decode(key, buf, result);
    if (!s.ok()) {
        return s;
    }
    assert(result != nullptr);

    *matched = true;
    if (filters_.empty()) {
        return Status::OK();
    } else {
        return matchRow(*result, filters_, matched);
    }
}

Status TxnRowDecoder::decode(const std::string& key, const std::string& buf, TxnRowValue* row) {
    return Status(Status::kNotSupported);
}


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
