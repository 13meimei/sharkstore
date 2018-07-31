_Pragma("once");

#include "proto/gen/kvrpcpb.pb.h"
#include "table.h"

namespace sharkstore {
namespace test {
namespace helper {

class SelectResultParser {
public:
    SelectResultParser(const kvrpcpb::SelectRequest& req,
                       const kvrpcpb::SelectResponse& resp);

    const std::vector<std::vector<std::string>>& GetRows() const {
        return rows_;
    }

    const std::vector<std::string>& GetKeys() const {
        return keys_;
    }

private:
    std::vector<std::vector<std::string>> rows_;
    std::vector<std::string> keys_;
};


} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
