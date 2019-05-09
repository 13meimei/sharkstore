_Pragma("once");

#include <rocksdb/db.h>
#include "proto/gen/kvrpcpb.pb.h"
#include "kv_fetcher.h"
#include "row_decoder.h"
#include "store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RowFetcher {
public:
    RowFetcher(Store& s, const kvrpcpb::SelectRequest& req);
    RowFetcher(Store& s, const kvrpcpb::UpdateRequest& req);
    RowFetcher(Store& s, const kvrpcpb::DeleteRequest& req);

    ~RowFetcher();

    RowFetcher(const RowFetcher&) = delete;
    RowFetcher& operator=(const RowFetcher&) = delete;

    Status Next(kvrpcpb::Row& row, bool& over);
    Status Next(txnpb::Row& row, bool& over);

private:
    Store& store_;
    RowDecoder decoder_;

    std::unique_ptr<KvFetcher> kv_fetcher_;
    std::unique_ptr<exprpb::Expr> where_filter_;
    bool matched_ = false;
    size_t iter_count_ = 0;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
