_Pragma("once");

#include "proto/gen/metapb.pb.h"
#include "table.h"
#include "storage/db/db_interface.h"

namespace sharkstore {
namespace test {
namespace helper {

// peer_id = node_id + 100
uint64_t GetPeerID(uint64_t node_id);

metapb::Range MakeRangeMeta(Table *t, size_t peers_num = 1);
metapb::Range MakeRangeMeta(Table *t, size_t peers_num, uint32_t rid);

// append '\x01' + table_id to buf
void EncodeKeyPrefix(std::string* buf, uint64_t table_id);

// append encoded pk value to buf
void EncodePrimaryKey(std::string *buf, const metapb::Column& col, const std::string& val);

// append encoded non-pk value to buf
void EncodeColumnValue(std::string *buf, const metapb::Column& col, const std::string& val);

void DecodeColumnValue(const std::string& buf, size_t& offset, const metapb::Column& col, std::string *val);

Status OpenDB(const std::string& path, dataserver::storage::DbInterface **db_ptr);

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
