_Pragma("once");

#include "proto/gen/metapb.pb.h"
#include "table.h"

namespace sharkstore {
namespace test {
namespace helper {

metapb::Range MakeRangeMeta(Table *t);

// append '\x01' + table_id to buf
void EncodeKeyPrefix(std::string* buf, uint64_t table_id);

// append encoded pk value to buf
void EncodePrimaryKey(std::string *buf, const metapb::Column& col, const std::string& val);

// append encoded non-pk value to buf
void EncodeColumnValue(std::string *buf, const metapb::Column& col, const std::string& val);

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
