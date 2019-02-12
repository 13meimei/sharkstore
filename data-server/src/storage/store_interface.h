//
// Created by young on 19-2-11.
//

#ifndef SHARKSTORE_DS_STORE_INTERFACE_H
#define SHARKSTORE_DS_STORE_INTERFACE_H

#include "iterator.h"
#include "metric.h"
#include "range/split_policy.h"

namespace sharkstore {
namespace dataserver {
namespace storage {

class StoreInterface {
public:
    StoreInterface() = default;
    virtual ~StoreInterface() = default;

    virtual Status Get(const std::string &key, std::string *value) = 0;

    virtual Status Put(const std::string &key, const std::string &value) = 0;

    virtual Status Delete(const std::string &key) = 0;

    virtual Status Insert(const kvrpcpb::InsertRequest &req, uint64_t *affected) = 0;

    virtual Status Update(const kvrpcpb::UpdateRequest &req, uint64_t *affected, uint64_t *update_bytes) = 0;

    virtual Status Select(const kvrpcpb::SelectRequest &req, kvrpcpb::SelectResponse *resp) = 0;

    virtual Status DeleteRows(const kvrpcpb::DeleteRequest &req, uint64_t *affected) = 0;

    virtual Status Truncate() = 0;


    virtual void SetEndKey(std::string end_key) = 0;

    virtual const std::vector <metapb::Column> &GetPrimaryKeys() const = 0;

    virtual void ResetMetric() = 0;

    virtual void CollectMetric(MetricStat *stat) = 0;

    // 统计存储实际大小，并且根据split_size返回中间key
    virtual Status StatSize(uint64_t split_size, range::SplitKeyMode mode,
                    uint64_t *real_size, std::string *split_key) = 0;

public:
    virtual IteratorInterface *NewIterator(const ::kvrpcpb::Scope &scope) = 0;

    virtual IteratorInterface *NewIterator(std::string start = std::string(),
                          std::string limit = std::string()) = 0;

    virtual Status BatchDelete(const std::vector <std::string> &keys) = 0;

    virtual bool KeyExists(const std::string &key) = 0;

    virtual Status BatchSet(
            const std::vector <std::pair<std::string, std::string>> &keyValues) = 0;

    virtual Status RangeDelete(const std::string &start, const std::string &limit) = 0;

    virtual Status ApplySnapshot(const std::vector <std::string> &datas) = 0;
};

} //namespace sharkstore
} //namespace dataserver
} //namespace storage

#endif //SHARKSTORE_DS_STORE_INTERFACE_H
