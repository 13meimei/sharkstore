#include "query_parser.h"

#include "helper_util.h"

namespace sharkstore {
namespace test {
namespace helper {

SelectResultParser::SelectResultParser(const kvrpcpb::SelectRequest& req,
                                       const kvrpcpb::SelectResponse& resp) {
    metapb::Column fake_count_col;
    fake_count_col.set_data_type(metapb::BigInt);

    rows_.reserve(resp.rows_size());
    for (const auto& r: resp.rows()) {
        keys_.push_back(r.key());

        std::vector<std::string> values;
        size_t offset = 0;
        for (const auto& f: req.field_list()) {
            std::string val;
            if (f.typ() == kvrpcpb::SelectField_Type_Column) {
                DecodeColumnValue(r.fields(), offset, f.column(), &val);
            } else {
                if (f.aggre_func() == "count") {
                    DecodeColumnValue(r.fields(), offset, fake_count_col, &val);
                } else {
                    DecodeColumnValue(r.fields(), offset, f.column(), &val);
                }
            }
            values.push_back(std::move(val));
        }
        rows_.push_back(std::move(values));
    }
}


} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
