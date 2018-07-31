#include "util.h"

namespace sharkstore {
namespace test {
namespace helper  {

metapb::Range MakeRangeMeta(Table *t) {
    metapb::Range meta;
    meta.set_id(1);
    meta.set_start_key(std::string("\x00", 1));
    meta.set_end_key("\xff");
    meta.mutable_range_epoch()->set_version(1);
    meta.mutable_range_epoch()->set_conf_ver(1);

    meta.set_table_id(t->GetID());
    auto pks = t->GetPKs();
    if (pks.size() == 0) {
        throw std::runtime_error("invalid table(no primary key)");
    }
    for (const auto& pk : pks) {
        auto p = meta.add_primary_keys();
        p->CopyFrom(pk);
    }

    return meta;
}

} /* namespace helper */
} /* namespace test  */
} /* namespace sharkstore */

