_Pragma("once");

#include "base/status.h"
#include "proto/gen/metapb.pb.h"
#include "field_value.h"
#include "row_decoder.h"
#include "select_txn.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

uint64_t calExpireAt(uint64_t ttl);
bool isExpired(uint64_t expired_at);

Status decodePK(const std::string& key, size_t& offset, const metapb::Column& col,
        std::unique_ptr<FieldValue>* field);

Status decodeField(const std::string& buf, size_t& offset, const metapb::Column& col,
        std::unique_ptr<FieldValue>& field);

Status parseThreshold(const std::string& thres, const metapb::Column& col,
        std::unique_ptr<FieldValue>& field);

Status matchField(FieldValue* field, const kvrpcpb::Match& filter, bool& matched);
Status matchRow(const RowResult& row, const std::vector<kvrpcpb::Match>& filters, bool& matched);
Status matchRow(const TxnRowValue& row, const std::vector<kvrpcpb::Match>& filters, bool& matched);
Status matchRow(const TxnRowValue& row, const std::shared_ptr<CWhereExpr> filter, bool& matched);

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
