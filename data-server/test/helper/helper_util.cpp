#include "helper_util.h"

#include <fastcommon/logger.h>
#include <fastcommon/shared_func.h>

#include "common/ds_encoding.h"
#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/skiplist_impl/skiplist_impl.h"
#include "storage/db/mass_tree_impl/mass_tree_mvcc.h"

namespace sharkstore {
namespace test {
namespace helper {

using namespace sharkstore::dataserver;

uint64_t GetPeerID(uint64_t node_id) {
    return node_id + 100;
}

metapb::Range MakeRangeMeta(Table *t, size_t peers_num) {
    const uint32_t RID = 1;
    return MakeRangeMeta(t, peers_num, RID);
}

metapb::Range MakeRangeMeta(Table *t, size_t peers_num, uint32_t rid) {
    metapb::Range meta;
    meta.set_id(rid);
    EncodeKeyPrefix(meta.mutable_start_key(), t->GetID());
    EncodeKeyPrefix(meta.mutable_end_key(), t->GetID() + 1);

    for (size_t i = 0; i < peers_num; ++i) {
        auto peer = meta.add_peers();
        peer->set_node_id(i + 1);
        peer->set_id(GetPeerID(peer->node_id()));
        peer->set_type(metapb::PeerType_Normal);
    }
    meta.mutable_range_epoch()->set_version(peers_num);
    meta.mutable_range_epoch()->set_conf_ver(peers_num);

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

static const char kKeyPrefixByte = '\x01';
static const char kIndexPrefixByte = '\x02';

void EncodeKeyPrefix(std::string *buf, uint64_t table_id) {
    buf->push_back(kKeyPrefixByte);
    EncodeUint64Ascending(buf, table_id);
}

// append encoded pk values to buf
void EncodePrimaryKey(std::string *buf, const metapb::Column& col, const std::string& val) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(val.c_str(), NULL, 10);
                EncodeVarintAscending(buf, i);
            } else {
                uint64_t i = strtoull(val.c_str(), NULL, 10);
                EncodeUvarintAscending(buf, i);
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = strtod(val.c_str(), NULL);
            EncodeFloatAscending(buf, d);
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            EncodeBytesAscending(buf, val.c_str(), val.size());
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodePrimaryKey: invalid column data type: ") +
                std::to_string(static_cast<int>(col.data_type())));
    }
}

void EncodeColumnValue(std::string *buf, const metapb::Column& col, const std::string& val) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            if (!col.unsigned_()) {
                int64_t i = strtoll(val.c_str(), NULL, 10);
                EncodeIntValue(buf, static_cast<uint32_t>(col.id()), i);
            } else {
                uint64_t i = strtoull(val.c_str(), NULL, 10);
                EncodeIntValue(buf, static_cast<uint32_t>(col.id()), static_cast<int64_t>(i));
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = strtod(val.c_str(), NULL);
            EncodeFloatValue(buf, static_cast<uint32_t>(col.id()), d);
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            EncodeBytesValue(buf, static_cast<uint32_t>(col.id()), val.c_str(), val.size());
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                     std::to_string(static_cast<int>(col.data_type())));
    }
}

void DecodeColumnValue(const std::string& buf, size_t& offset, const metapb::Column& col, std::string *val) {
    switch (col.data_type()) {
        case metapb::Tinyint:
        case metapb::Smallint:
        case metapb::Int:
        case metapb::BigInt: {
            int64_t i = 0;
            DecodeIntValue(buf, offset, &i);
            if (col.unsigned_()) {
                *val = std::to_string(static_cast<uint64_t>(i));
            } else {
                *val = std::to_string(i);
            }
            break;
        }

        case metapb::Float:
        case metapb::Double: {
            double d = 0.0;
            DecodeFloatValue(buf, offset, &d);
            *val = std::to_string(d);
            break;
        }

        case metapb::Varchar:
        case metapb::Binary:
        case metapb::Date:
        case metapb::TimeStamp: {
            DecodeBytesValue(buf, offset, val);
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                     std::to_string(static_cast<int>(col.data_type())));
    }
}

void InitLog() {
    auto str = ::getenv("LOG_LEVEL");
    std::string log_level = "info";
    if (str != nullptr) {
        log_level = str;
    }
    log_init2();
    set_log_level(const_cast<char *>(log_level.c_str()));
}

Status OpenDB(const std::string& path, dataserver::storage::DbInterface **db_ptr) {
    auto db_env = ::getenv("DB");
    std::string db_name = "rocksdb";
    if (db_env != nullptr) {
        db_name = db_env;
    }
    if (db_name == "rocksdb") {
        std::cout << "Create rocksdb ...." << std::endl;
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
        column_families.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
        column_families.emplace_back("txn", rocksdb::ColumnFamilyOptions());
        rocksdb::Options ops;
        ops.create_if_missing = true;
        ops.error_if_exists = true;
        *db_ptr = new dataserver::storage::RocksDBImpl(ops, path);
    } else if (db_name == "mass-tree" || db_name == "mass") {
        std::cout << "Create mass-tree db ...." << std::endl;
        *db_ptr = new dataserver::storage::MvccMassTree();
    } else if (db_name == "skiplist" || db_name == "skip") {
        std::cout << "Create skip list db ...." << std::endl;
        *db_ptr = new dataserver::storage::SkipListDBImpl();
    } else {
        return Status(Status::kNotSupported, "open db", db_name);
    }
    return (*db_ptr)->Open();
}

} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */

