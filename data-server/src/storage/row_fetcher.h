_Pragma("once");

#include <rocksdb/db.h>
#include "proto/gen/kvrpcpb.pb.h"
#include "row_decoder.h"
#include "store.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class RowFetcher {
public:
    RowFetcher(StoreInterface& s, const kvrpcpb::SelectRequest& req);
    RowFetcher(StoreInterface& s, const kvrpcpb::UpdateRequest& req);
    RowFetcher(StoreInterface& s, const kvrpcpb::DeleteRequest& req);

    ~RowFetcher();

    RowFetcher(const RowFetcher&) = delete;
    RowFetcher& operator=(const RowFetcher&) = delete;

    Status Next(RowResult* result, bool* over);

private:
    void init(const std::string& key, const ::kvrpcpb::Scope& scope);
    Status nextOneKey(RowResult* result, bool* over);
    Status nextScope(RowResult* result, bool* over);

private:
    StoreInterface& store_;
    RowDecoder decoder_;

    std::string key_;
    IteratorInterface* iter_ = nullptr;
    Status last_status_;
    bool matched_ = false;
    size_t iter_count_ = 0;
};

} /* namespace storage */
} /* namespace dataserver */
} /* namespace sharkstore */
