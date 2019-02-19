_Pragma("once");

#include "base/status.h"
#include "proto/gen/metapb.pb.h"
#include "field_value.h"
#include "row_decoder.h"
#include "select_txn.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

Status decodePK(const std::string& key, size_t& offset, const metapb::Column& col, FieldValue** value);
Status decodeField(const std::string& buf, size_t& offset, const metapb::Column& col, FieldValue** value);

Status parseThreshold(const std::string& thres, const metapb::Column& col, std::unique_ptr<FieldValue>* value);

Status matchField(FieldValue* field, const kvrpcpb::Match& filter, bool* matched);
Status matchRow(const RowResult& row, const std::vector<kvrpcpb::Match>& filters, bool *matched);
Status matchRow(const TxnRowValue& row, const std::vector<kvrpcpb::Match>& filters, bool *matched);


} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
