#include "range_server.h"

#include <chrono>
#include <future>
#include <thread>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <rocksdb/advanced_options.h>
#include <rocksdb/cache.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/db_ttl.h>
#include <rocksdb/utilities/blob_db/blob_db.h>
#include <rocksdb/rate_limiter.h>
#include <fastcommon/shared_func.h>
#include <common/ds_config.h>

#include "base/util.h"
#include "common/ds_config.h"
#include "common/ds_encoding.h"
#include "frame/sf_logger.h"
#include "proto/gen/funcpb.pb.h"
#include "proto/gen/metapb.pb.h"
#include "proto/gen/schpb.pb.h"
#include "proto/gen/txn.pb.h"
#include "storage/metric.h"
#include "run_status.h"
#include "monitor/statistics.h"

#include "storage/db/rocksdb_impl/rocksdb_impl.h"
#include "storage/db/skiplist_impl/skiplist_impl.h"
#ifdef SHARK_USE_BWTREE
#include "storage/db/bwtree_impl/bwtree_db_impl.h"
#endif

#include "server.h"
#include "range_context_impl.h"

namespace sharkstore {
namespace dataserver {
namespace server {

static const std::string kMetaPathSuffix = "meta";

using namespace sharkstore::dataserver::range;

template <class RequestT, class ResponseT, class RangeFuncPointer>
void RangeServer::ForwardToRange(RPCRequestPtr& rpc, RangeFuncPointer func_ptr) {
    RequestT request;
    if (!rpc->ParseTo(request)) {
        FLOG_ERROR("deserialize %s request failed, from %s, msg id=%" PRIu64,
                rpc->FuncName().c_str(), rpc->ctx.remote_addr.c_str(), rpc->MsgID());
        return;
    }

    FLOG_DEBUG("%s from %s called. req: %s", rpc->FuncName().c_str(),
            rpc->ctx.remote_addr.c_str(), request.DebugString().c_str());

    // check timeout
    if (rpc->expire_time != 0 && rpc->expire_time < NowMilliSeconds()) {
        FLOG_WARN("%s request timeout from %s", rpc->FuncName().c_str(), rpc->ctx.remote_addr.c_str());
        ResponseT response;
        TimeOut(request.header(), response.mutable_header());
        rpc->Reply(response);
        return;
    }

    auto range = Find(request.header().range_id());
    if (range != nullptr) {
        ((*range).*func_ptr)(std::move(rpc), request);
        return;
    } else {
        FLOG_ERROR("%s request not found range_id %" PRIu64 " failed", rpc->FuncName().c_str(),
                   request.header().range_id());
        ResponseT response;
        RangeNotFound(request.header(), response.mutable_header());
        rpc->Reply(response);
        return;
    }
}

int RangeServer::Init(ContextServer *context) {
    FLOG_INFO("RangeServer Init begin ...");

    context_ = context;

    // 打开数据db
    if (OpenDB() != 0) {
        FLOG_ERROR("RangeServer Init error ...");
        return -1;
    }
    context_->db = db_;

    // 打开meta db
    auto meta_path = JoinFilePath({ds_config.rocksdb_config.path, kMetaPathSuffix});
    meta_store_ = new storage::MetaStore(meta_path);
    auto ret = meta_store_->Open();
    if (!ret.ok()) {
        FLOG_ERROR("open meta store failed(%s), path=%s", ret.ToString().c_str(),
                   meta_path.c_str());
        return -1;
    }
    // 保存一下NodeId 到Meta
    assert(context_->node_id != 0);
    ret = meta_store_->SaveNodeID(context_->node_id);
    if (!ret.ok()) {
        FLOG_ERROR("save node id to meta failed(%s)", ret.ToString().c_str());
        return -1;
    } else {
        FLOG_DEBUG("save node_id (%" PRIu64 ") to meta.", context_->node_id);
    }
    context_->meta_store = meta_store_;

    // 创建RangeContext
    range_context_.reset(new RangeContextImpl(context_));

    // 初始化WatchServer
    // TODO: enable
    watch_server_ = new watch::WatchServer(ds_config.watch_config.watcher_set_size);

    std::vector<metapb::Range> range_metas;
    ret = meta_store_->GetAllRange(&range_metas);
    if (!ret.ok()) {
        FLOG_ERROR("load range metas failed(%s)", ret.ToString().c_str());
        return -1;
    }
    if (recover(range_metas) != 0) {
        FLOG_ERROR("load local range meta failed");
        return -1;
    }

    FLOG_INFO("RangeServer Init end ...");

    return 0;
}

int RangeServer::Start() {
    FLOG_INFO("RangeServer Start begin ...");

    range_heartbeat_ = std::thread(&RangeServer::Heartbeat, this);

    auto handle = range_heartbeat_.native_handle();
    AnnotateThread(handle, "range_hb");

    char name[32] = {'\0'};
    for (int i = 0; i < ds_config.range_config.worker_threads; i++) {
        worker_.emplace_back([this] {
            uint64_t range_id = 0;
            while (g_continue_flag) {
                {
                    std::unique_lock<std::mutex> lock(statis_mutex_);
                    if (statis_queue_.empty()) {
                        statis_cond_.wait_for(lock, std::chrono::seconds(5));
                        continue;
                    }

                    range_id = statis_queue_.front();
                    statis_queue_.pop();
                }

                auto range = Find(range_id);
                if (range == nullptr) {
                    FLOG_ERROR("RawPut request not found range_id %" PRIu64 " failed",
                               range_id);
                    continue;
                }
                range->ResetStatisSize();
            }

            FLOG_INFO("StatisSize worker thread exit...");
        });

        auto handle = worker_[i].native_handle();
        snprintf(name, 32, "statis:%d", i);
        AnnotateThread(handle, name);
    }

    FLOG_INFO("RangeServer Start end ...");
    return 0;
}

void RangeServer::Stop() {
    FLOG_INFO("RangeServer Stop begin ...");

    queue_cond_.notify_all();
    statis_cond_.notify_all();

    for (auto &work : worker_) {
        if (work.joinable()) {
            work.join();
        }
    }

    if (range_heartbeat_.joinable()) {
        range_heartbeat_.join();
    }

    CloseDB();

    auto it = ranges_.begin();
    while (it != ranges_.end()) {
        it->second->Shutdown();
        it = ranges_.erase(it);
    }

    if (meta_store_ != nullptr) {
        delete meta_store_;
        meta_store_ = nullptr;
    }

    FLOG_INFO("RangeServer Stop end ...");
}

int RangeServer::OpenDB() {
    std::string engine_name(ds_config.engine_config.name);
    if (strcasecmp(engine_name.c_str(), "rocksdb") == 0) {
        print_rocksdb_config();
        db_ = new storage::RocksDBImpl(ds_config.rocksdb_config);
    } else if (strcasecmp(engine_name.c_str(), "memory") == 0) {
        db_ = new storage::SkipListDBImpl();
    } else if (strcasecmp(engine_name.c_str(), "bwtree") == 0) {
#ifdef SHARK_USE_BWTREE
        db_ = new storage::BwTreeDBImpl();
#else
        FLOG_ERROR("bwtree is not enabled. confirm build opition ENABLE_BWTREE is on");
        return -1;
#endif
    } else {
        FLOG_ERROR("unknown engine name: %s", engine_name.c_str());
    }

    auto s = db_->Open();
    if (!s.ok()) {
        FLOG_ERROR("open %s db failed: %s", engine_name.c_str(), s.ToString().c_str());
        return -1;
    } else {
        FLOG_INFO("open %s db successfully", engine_name.c_str());
    }
    return 0;
}

void RangeServer::CloseDB() {
    if (db_ != nullptr) {
        delete db_;
        db_ = nullptr;
    }
}

void RangeServer::Clear() {
    FLOG_WARN("clear range data!");

    RemoveDirAll(ds_config.raft_config.log_path);
    RemoveDirAll(ds_config.rocksdb_config.path);
}

void RangeServer::DealTask(RPCRequestPtr rpc) {
    context_->run_status->PushTime(monitor::HistogramType::kQWait, NowMicros() - rpc->begin_time);

    const auto& header = rpc->msg->head;

    FLOG_DEBUG(
        "server start deal %s task from %s, msgid=%" PRId64,
        funcpb::FunctionID_Name(static_cast<funcpb::FunctionID>(header.func_id)).c_str(),
        rpc->ctx.remote_addr.c_str(), header.msg_id);

    switch (header.func_id) {
        // server level methods
        case funcpb::kFuncCreateRange:
            CreateRange(*rpc);
            break;
        case funcpb::kFuncDeleteRange:
            DeleteRange(*rpc);
            break;
        case funcpb::kFuncRangeTransferLeader:
            TransferLeader(*rpc);
            break;
        case funcpb::kFuncReplaceRange:
            ReplaceRange(*rpc);
            break;
        case funcpb::kFuncOfflineRange:
            OfflineRange(*rpc);
            break;
        case funcpb::kFuncGetPeerInfo:
            GetPeerInfo(*rpc);
            break;
        case funcpb::kFuncSetNodeLogLevel:
            SetLogLevel(*rpc);
            break;

        // Raw KV methods
        case funcpb::kFuncRawGet:
            ForwardToRange<kvrpcpb::DsKvRawGetRequest, kvrpcpb::DsKvRawGetResponse>(rpc, &Range::RawGet);
            break;
        case funcpb::kFuncRawPut:
            ForwardToRange<kvrpcpb::DsKvRawPutRequest, kvrpcpb::DsKvRawPutResponse>(rpc, &Range::RawPut);
            break;
        case funcpb::kFuncRawDelete:
            ForwardToRange<kvrpcpb::DsKvRawDeleteRequest, kvrpcpb::DsKvRawDeleteResponse>(rpc, &Range::RawDelete);
            break;

        // SQL methods
        case funcpb::kFuncInsert:
            ForwardToRange<kvrpcpb::DsInsertRequest, kvrpcpb::DsInsertResponse>(rpc, &Range::Insert);
            break;
        case funcpb::kFuncUpdate:
            ForwardToRange<kvrpcpb::DsUpdateRequest, kvrpcpb::DsUpdateResponse>(rpc, &Range::Update);
            break;
        case funcpb::kFuncSelect:
            ForwardToRange<kvrpcpb::DsSelectRequest, kvrpcpb::DsSelectResponse>(rpc, &Range::Select);
            break;
        case funcpb::kFuncDelete:
            ForwardToRange<kvrpcpb::DsDeleteRequest, kvrpcpb::DsDeleteResponse>(rpc, &Range::Delete);
            break;

        // Watch methods
        case funcpb::kFuncWatchGet:
            ForwardToRange<watchpb::DsWatchRequest, watchpb::DsWatchResponse>(rpc, &Range::WatchGet);
            break;
        case funcpb::kFuncPureGet:
            ForwardToRange<watchpb::DsKvWatchGetMultiRequest, watchpb::DsKvWatchGetMultiResponse>(rpc, &Range::PureGet);
            break;
        case funcpb::kFuncWatchPut:
            ForwardToRange<watchpb::DsKvWatchPutRequest, watchpb::DsKvWatchPutResponse>(rpc, &Range::WatchPut);
            break;
        case funcpb::kFuncWatchDel:
            ForwardToRange<watchpb::DsKvWatchDeleteRequest, watchpb::DsKvWatchDeleteResponse>(rpc, &Range::WatchDel);
            break;

        // lock methods
        case funcpb::kFuncLock:
            ForwardToRange<kvrpcpb::DsLockRequest, kvrpcpb::DsLockResponse>(rpc, &Range::Lock);
            break;
        case funcpb::kFuncLockUpdate:
            ForwardToRange<kvrpcpb::DsLockUpdateRequest, kvrpcpb::DsLockUpdateResponse>(rpc, &Range::LockUpdate);
            break;
        case funcpb::kFuncUnlock:
            ForwardToRange<kvrpcpb::DsUnlockRequest, kvrpcpb::DsUnlockResponse>(rpc, &Range::Unlock);
            break;
        case funcpb::kFuncUnlockForce:
            ForwardToRange<kvrpcpb::DsUnlockForceRequest, kvrpcpb::DsUnlockForceResponse>(rpc, &Range::UnlockForce);
            break;
        case funcpb::kFuncLockWatch:
            ForwardToRange<watchpb::DsWatchRequest, watchpb::DsWatchResponse>(rpc, &Range::LockWatch);
            break;
        case funcpb::kFuncLockGet:
            ForwardToRange<kvrpcpb::DsLockGetRequest, kvrpcpb::DsLockGetResponse>(rpc, &Range::LockGet);
            break;

        // redis method
        case funcpb::kFuncKvSet:
            ForwardToRange<kvrpcpb::DsKvSetRequest, kvrpcpb::DsKvSetResponse>(rpc, &Range::KVSet);
            break;
        case funcpb::kFuncKvGet:
            ForwardToRange<kvrpcpb::DsKvGetRequest, kvrpcpb::DsKvGetResponse>(rpc, &Range::KVGet);
            break;
        case funcpb::kFuncKvBatchSet:
            ForwardToRange<kvrpcpb::DsKvBatchSetRequest, kvrpcpb::DsKvBatchSetResponse>(rpc, &Range::KVBatchSet);
            break;
        case funcpb::kFuncKvBatchGet:
            ForwardToRange<kvrpcpb::DsKvBatchGetRequest, kvrpcpb::DsKvBatchGetResponse>(rpc, &Range::KVBatchGet);
            break;
        case funcpb::kFuncKvDel:
            ForwardToRange<kvrpcpb::DsKvDeleteRequest, kvrpcpb::DsKvDeleteResponse>(rpc, &Range::KVDelete);
            break;
        case funcpb::kFuncKvBatchDel:
            ForwardToRange<kvrpcpb::DsKvBatchDeleteRequest, kvrpcpb::DsKvBatchDeleteResponse>(rpc, &Range::KVBatchDelete);
            break;
        case funcpb::kFuncKvRangeDel:
            ForwardToRange<kvrpcpb::DsKvRangeDeleteRequest, kvrpcpb::DsKvRangeDeleteResponse>(rpc, &Range::KVRangeDelete);
            break;
        case funcpb::kFuncKvScan:
            ForwardToRange<kvrpcpb::DsKvScanRequest, kvrpcpb::DsKvScanResponse>(rpc, &Range::KVScan);
            break;

        // TXN methods
        case funcpb::kFuncTxnPrepare:
            ForwardToRange<txnpb::DsPrepareRequest, txnpb::DsPrepareResponse>(rpc, &Range::TxnPrepare);
            break;
        case funcpb::kFuncTxnDecide:
            ForwardToRange<txnpb::DsDecideRequest, txnpb::DsDecideResponse>(rpc, &Range::TxnDecide);
            break;
        case funcpb::kFuncTxnClearup:
            ForwardToRange<txnpb::DsClearupRequest, txnpb::DsClearupResponse>(rpc, &Range::TxnClearup);
            break;
        case funcpb::kFuncTxnGetLockInfo:
            ForwardToRange<txnpb::DsGetLockInfoRequest, txnpb::DsGetLockInfoResponse>(rpc, &Range::TxnGetLockInfo);
            break;
        case funcpb::kFuncTxnSelect:
            ForwardToRange<txnpb::DsSelectRequest, txnpb::DsSelectResponse>(rpc, &Range::TxnSelect);
            break;
        default:
            FLOG_ERROR("func id is Invalid %d", header.func_id);
    }
}

void RangeServer::CreateRange(RPCRequest& req) {
    schpb::CreateRangeRequest create_req;
    if (!req.ParseTo(create_req)) {
        FLOG_ERROR("deserialize create range request failed");
        return;
    }

    FLOG_INFO("range[%" PRIu64 "] recv create range from master", create_req.range().id());

    errorpb::Error *err = nullptr;
    schpb::CreateRangeResponse create_resp;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        auto it = ranges_.find(create_req.range().id());
        if (it != ranges_.end()) {
            FLOG_WARN("range[%" PRIu64 "] already exist.", create_req.range().id());

            if (!it->second->EpochIsEqual(create_req.range())) {
                err = new errorpb::Error;
                auto meta = new metapb::Range(it->second->options());
                err->mutable_stale_range()->set_allocated_range(meta);
                err->set_message("range already exist but epoch no equal");
            }
            break;
        }

        auto ret = meta_store_->AddRange(create_req.range());
        if (!ret.ok()) {
            err = new errorpb::Error;
            err->set_message("create range seriaize meta failed");

            FLOG_ERROR("create range save meta failed");
            break;
        }

        ret = CreateRange(create_req.range());
        if (!ret.ok() && ret.code() != Status::kDuplicate) {
            err = new errorpb::Error;
            err->set_message(ret.ToString());

            meta_store_->DelRange(create_req.range().id());
            FLOG_ERROR("create range failed %" PRIu64, create_req.range().id());
            break;
        }

    } while (false);

    if (err != nullptr) {
        create_resp.mutable_header()->set_allocated_error(err);
    }
    req.Reply(create_resp);
}

Status RangeServer::CreateRange(const metapb::Range &range, uint64_t leader,
        uint64_t log_start_index) {
    FLOG_DEBUG("new range: id=%" PRIu64 ", start=%s, end=%s,"
               " version=%" PRIu64 ", conf_ver=%" PRIu64,
               range.id(), EncodeToHexString(range.start_key()).c_str(),
               EncodeToHexString(range.end_key()).c_str(), range.range_epoch().version(),
               range.range_epoch().conf_ver());

    if (range.peers_size() == 0) {
        FLOG_ERROR("CreateRange range[%" PRIu64 "] failed. peers is zero", range.id());
        return Status(Status::kInvalidArgument, "invalid peer size", "0");
    }

    auto it = ranges_.find(range.id());
    if (it != ranges_.end()) {
        FLOG_WARN("CreateRange range[%" PRIu64 "] is exist.", range.id());
        return Status(Status::kDuplicate, "range is exist", "");
    }

    auto rng = std::make_shared<range::Range>(range_context_.get(), range);
    // 初始化range
    auto ret = rng->Initialize(leader, log_start_index);
    if (!ret.ok()) {
        FLOG_ERROR("initialize range[%" PRIu64 "] failed: %s", range.id(),
                   ret.ToString().c_str());
        return ret;
    }

    ranges_[range.id()] = rng;

    FLOG_INFO("create new range[%" PRIu64 "] success.", range.id());

    return ret;
}

void RangeServer::DeleteRange(RPCRequest& req) {
    schpb::DeleteRangeRequest del_req;
    if (!req.ParseTo(del_req)) {
        FLOG_ERROR("deserialize delete range request failed");
        return;
    }

    FLOG_WARN("range[%" PRIu64 "] recv DeleteRange request. peer_id=%" PRIu64,
            del_req.range_id(), del_req.peer_id());

    schpb::DeleteRangeResponse del_resp;
    auto s = DeleteRange(del_req.range_id(), del_req.peer_id());
    if (!s.ok()) {
        FLOG_ERROR("range[%" PRIu64 "] delete failed: %s", del_req.range_id(), s.ToString().c_str());
        auto err = del_resp.mutable_header()->mutable_error();
        err->set_message(s.ToString());
    }
    req.Reply(del_resp);
}

Status RangeServer::DeleteRange(uint64_t range_id, uint64_t peer_id) {
    std::shared_ptr<range::Range> rng;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        auto it = ranges_.find(range_id);
        if (it == ranges_.end()) {
            FLOG_WARN("delete range[%" PRIu64 "] not found.", range_id);
            return Status::OK();
        }
        rng = it->second;

        auto local_peer_id = rng->GetPeerID();
        // local_peer_id == 0 maybe removed
        if (peer_id != 0 && local_peer_id != 0 && local_peer_id != peer_id) {
            FLOG_WARN("range[%" PRIu64 "] delete a mismached peer. local: %" PRIu64 ", request: %" PRIu64,
                    range_id, local_peer_id, peer_id);
            // consider mismatch as success
            return Status::OK();
        }

        meta_store_->DelRange(range_id);
        auto s = rng->Destroy();
        if (!s.ok()) {
            FLOG_INFO("delete range[%" PRIu64 "] truncate failed.", range_id);
            return s;
        } else {
            ranges_.erase(it);
        }
    } while (false);

    FLOG_INFO("delete range[%" PRIu64 "] success.", range_id);

    return Status::OK();
}

void RangeServer::OfflineRange(RPCRequest& req) {
    schpb::OfflineRangeRequest off_req;
    if (!req.ParseTo(off_req)) {
        FLOG_ERROR("deserialize offline range request failed");
        return;
    }

    schpb::OfflineRangeResponse off_resp;
    if (OfflineRange(off_req.rangeid()) != 0) {
        auto err = off_resp.mutable_header()->mutable_error();
        err->set_message("offline range failed");
    }

    req.Reply(off_resp);
}

int RangeServer::OfflineRange(uint64_t range_id) {
    FLOG_INFO("offline range[%" PRIu64 "] success.", range_id);

    std::shared_ptr<range::Range> rng;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        auto it = ranges_.find(range_id);
        if (it == ranges_.end()) {
            FLOG_WARN("offline range[%" PRIu64 "] not found.", range_id);
            return -1;
        }

        rng = it->second;

        // TODO::close range
        // current: Shutdown replaced
        rng->Shutdown();
        ranges_.erase(it);

    } while (false);

    return 0;
}

int RangeServer::CloseRange(uint64_t range_id) {
    std::shared_ptr<range::Range> rng;
    do {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);

        meta_store_->DelRange(range_id);

        auto it = ranges_.find(range_id);
        if (it == ranges_.end()) {
            FLOG_WARN("close range[%" PRIu64 "] not found.", range_id);
            return 0;
        }

        rng = it->second;

        // TODO::close range
        // current: Shutdown replaced
        rng->Shutdown();
        ranges_.erase(it);

    } while (false);

    FLOG_INFO("close range[%" PRIu64 "] success.", range_id);

    return 0;
}

void RangeServer::ReplaceRange(RPCRequest& req) {
    schpb::ReplaceRangeRequest replace_req;
    if (!req.ParseTo(replace_req)) {
        FLOG_ERROR("deserialize replace range request failed");
        return;
    }

    FLOG_WARN("start update range. old=%" PRIu64 ", new=%" PRIu64, replace_req.old_range_id(),
            replace_req.new_range().id());

    schpb::ReplaceRangeResponse replace_resp;
    do {
        if (CloseRange(replace_req.old_range_id()) != 0) {
            auto err = replace_resp.mutable_header()->mutable_error();
            err->set_message("close old range failed");
            break;
        }

        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
        auto ret = CreateRange(replace_req.new_range());
        if (!ret.ok()) {
            auto err = replace_resp.mutable_header()->mutable_error();
            err->set_message("create range failed");
        }
    } while (false);

    req.Reply(replace_resp);
}

void RangeServer::TransferLeader(RPCRequest& req) {
    schpb::TransferRangeLeaderRequest transfer_req;
    if (!req.ParseTo(transfer_req)) {
        FLOG_ERROR("deserialize transfer leader request failed");
        return;
    }

    schpb::TransferRangeLeaderResponse transfer_resp;
    auto range = Find(transfer_req.range_id());
    if (range == nullptr) {
        FLOG_ERROR("TransferLeade request not found range_id %" PRIu64 " failed",
                   transfer_req.range_id());
    } else {
        range->TransferLeader();
    }

    req.Reply(transfer_resp);
}

void RangeServer::GetPeerInfo(RPCRequest& req) {
    schpb::GetPeerInfoRequest get_req;
    if (!req.ParseTo(get_req)) {
        FLOG_ERROR("deserialize transfer leader request failed");
        return;
    }

    schpb::GetPeerInfoResponse get_resp;
    raft::RaftStatus peer_info;
    auto range = Find(get_req.range_id());
    if (range == nullptr) {
        FLOG_ERROR("TransferLeade request not found range_id %" PRIu64 " failed",
                   get_req.range_id());

        auto err = get_resp.mutable_header()->mutable_error();
        err->set_message("range not found");
        err->mutable_range_not_found()->set_range_id(get_req.range_id());
    } else {
        range->GetPeerInfo(&peer_info);
        get_resp.set_index(peer_info.index);
        get_resp.set_term(peer_info.term);
        get_resp.set_commit(peer_info.commit);

        auto replica = get_resp.mutable_replica();
        range->GetReplica(replica);
    }

    req.Reply(get_resp);
}

void RangeServer::SetLogLevel(RPCRequest& req) {
    schpb::SetNodeLogLevelRequest set_req;
    if (!req.ParseTo(set_req)) {
        FLOG_ERROR("deserialize transfer leader request failed");
        return;
    }

    schpb::SetNodeLogLevelResponse set_resp;
    char level[8];
    snprintf(level, 8, "%s", set_req.level().c_str());
    set_log_level(level);

    req.Reply(set_resp);
}

size_t RangeServer::GetRangesSize() const {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);
    return ranges_.size();
}

std::shared_ptr<range::Range> RangeServer::Find(uint64_t range_id) {
    sharkstore::shared_lock<sharkstore::shared_mutex> lock(rw_lock_);

    auto it = ranges_.find(range_id);
    if (it == ranges_.end()) {
        for (auto itr = ranges_.begin(); itr != ranges_.end(); itr++) {
            FLOG_DEBUG("current range cache range_id:%" PRIu64 " ", itr->first);
        }
        return nullptr;
    }

    if (!it->second->valid()) {
        FLOG_WARN("RangeHeartbeat range_id %" PRIu64 " is invalid", range_id);

        return nullptr;
    }

    return it->second;
}

Status RangeServer::SplitRange(uint64_t old_range_id, const raft_cmdpb::SplitRequest &req,
                  uint64_t raft_index) {
    auto rng = Find(old_range_id);
    if (rng == nullptr) {
        return Status(Status::kNotFound, "range not found", "");
    }

    bool is_exist = false;
    {
        std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
        auto ret = CreateRange(req.new_range(), req.leader(), raft_index + 1);
        if (ret.code() == Status::kDuplicate) {
            FLOG_WARN("range[%" PRIu64 "] ApplySplit(new range: %" PRIu64 ") already exist.",
                      old_range_id, req.new_range().id());
        } else if (!ret.ok()) {
            return ret;
        }
    }

    metapb::Range meta = rng->options();
    meta.set_end_key(req.split_key());
    meta.mutable_range_epoch()->set_version(req.epoch().version());

    std::vector<metapb::Range> batch_ranges{meta};
    if (!is_exist) {
        batch_ranges.push_back(req.new_range());
    }
    auto ret = meta_store_->BatchAddRange(batch_ranges);
    if (!ret.ok()) {
        if (!is_exist) {
            DeleteRange(req.new_range().id());
        }
    }
    return ret;
}

void RangeServer::TimeOut(const kvrpcpb::RequestHeader &req,
                          kvrpcpb::ResponseHeader *resp) {
    auto err = new errorpb::Error;
    err->set_message("time out");
    err->mutable_timeout();

    SetResponseHeader(resp, req, err);
}

void RangeServer::RangeNotFound(const kvrpcpb::RequestHeader &req,
                                kvrpcpb::ResponseHeader *resp) {
    auto err = new errorpb::Error;
    err->set_message("range not found");
    err->mutable_range_not_found()->set_range_id(req.range_id());

    SetResponseHeader(resp, req, err);
}

Status RangeServer::recover(const metapb::Range& meta) {
    auto rng = std::make_shared<range::Range>(range_context_.get(), meta);
    auto s = rng->Initialize(0);
    if (!s.ok()) return s;

    std::unique_lock<sharkstore::shared_mutex> lock(rw_lock_);
    auto ret = ranges_.emplace(meta.id(), rng);
    if (!ret.second) {
        return Status(Status::kDuplicate, "save range", std::to_string(meta.id()));
    }
    return Status::OK();
}

int RangeServer::recover(const std::vector<metapb::Range> &metas) {
    assert(ds_config.range_config.recover_concurrency > 0);
    auto actual_concurrency = std::min(metas.size() / 4 + 1,
                                       static_cast<size_t>(ds_config.range_config.recover_concurrency));
    if (actual_concurrency > 50)  {
        actual_concurrency = 50;
    }

    std::vector<std::future<Status>> recover_futures;
    std::vector<uint64_t> failed_ranges;
    std::mutex failed_mu;
    std::atomic<size_t> recover_pos = {0};
    std::atomic<size_t> success_counter = {0};

    FLOG_INFO("Start to recovery ranges. total ranges=%lu, concurrency=%lu, skip_fail=%d", metas.size(),
              actual_concurrency, ds_config.range_config.recover_skip_fail);

    auto begin = std::chrono::system_clock::now();

    for (size_t i = 0; i < actual_concurrency; ++i) {
        auto f = std::async(std::launch::async, [&, this] {
            while (true) {
                auto pos = recover_pos.fetch_add(1);
                if (pos >= metas.size()) {
                    return Status::OK();
                }
                const auto& meta = metas[pos];
                FLOG_DEBUG("Start Recover range id=%" PRIu64, meta.id());
                auto s = recover(meta);
                if (s.ok()) {
                    ++success_counter;
                } else {
                    FLOG_ERROR("Recovery range[%lu] failed: %s", meta.id(), s.ToString().c_str());
                    {
                        std::lock_guard<std::mutex> lock(failed_mu);
                        failed_ranges.push_back(meta.id());
                    }
                    if (ds_config.range_config.recover_skip_fail) { // allow failed
                        continue;
                    } else {
                        recover_pos = metas.size(); // failed, let other threads exit
                        return s;
                    }
                }
            }
        });
        recover_futures.push_back(std::move(f));
    }

    Status last_error;
    for (auto &f : recover_futures) {
        auto s = f.get();
        if (!s.ok()) last_error = s;
    }

    auto took_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - begin).count();

    if (!failed_ranges.empty()) {
        std::string failed_str;
        for (std::size_t i = 0; i < failed_ranges.size(); ++i)
            failed_str += std::to_string(failed_ranges[i]) + ", ";
        FLOG_ERROR("Range recovery failed ranges: [%s]", failed_str.c_str());
    }

    if (!last_error.ok()) {
        FLOG_ERROR("Range recovery abort, last status: %s. failed=%lu",
                  last_error.ToString().c_str(), failed_ranges.size());
        return -1;
    } else {
        FLOG_INFO("Range recovery finished. success=%lu, failed=%lu, time used=%lds%ldms",
                  success_counter.load(), failed_ranges.size(), took_ms / 1000, took_ms % 1000);
        return 0;
    }
}

void RangeServer::Heartbeat() {
    uint64_t range_id = 0;
    int interval = ds_config.hb_config.range_interval;

    while (g_continue_flag) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (range_heartbeat_queue_.empty()) {
                queue_cond_.wait_for(lock, std::chrono::seconds(interval));
                continue;
            }

            auto hb = range_heartbeat_queue_.top();

            time_t now = NowMilliSeconds();
            if (hb.first > now) {
                auto intval = std::chrono::milliseconds(hb.first - now);
                queue_cond_.wait_for(lock, intval);
                continue;
            }

            range_heartbeat_queue_.pop();
            range_id = hb.second;
        }

        auto range = Find(range_id);
        if (range != nullptr) {
            range->Heartbeat();
        }
    }

    FLOG_INFO("RangeHeartBeat thread exit...");
}

void RangeServer::LeaderQueuePush(uint64_t leader, time_t expire) {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    range_heartbeat_queue_.emplace(expire, leader);

    queue_cond_.notify_all();
}

void RangeServer::StatisPush(uint64_t range_id) {
    std::lock_guard<std::mutex> lock(statis_mutex_);
    statis_queue_.push(range_id);
    statis_cond_.notify_all();
}

void RangeServer::OnNodeHeartbeatResp(const mspb::NodeHeartbeatResponse &resp) {
    // TODO: clear replicas
    FLOG_INFO("Recv NodeHeartbeat Response from master server.");
}

void RangeServer::OnRangeHeartbeatResp(const mspb::RangeHeartbeatResponse &resp) {
    if (!resp.has_task()) {
        return;
    }

    auto range = Find(resp.range_id());
    if (range == nullptr) {
        FLOG_ERROR("RangeHeartbeat Task not found range_id %" PRIu64 " failed",
                   resp.range_id());
        return;
    }

    if (!range->valid()) {
        FLOG_WARN("RangeHeartbeat range_id %" PRIu64 " is invalid", resp.range_id());
        return;
    }

    switch (resp.task().type()) {
        case taskpb::TaskType::EmptyTask:
            FLOG_DEBUG("RangeHeartbeat task empty.");
            break;
        case taskpb::TaskType::RangeMerge:
            FLOG_DEBUG("RangeHeartbeat task RangeMerge.");
            // TODO
            break;
        case taskpb::TaskType::RangeDelete:
            FLOG_INFO("RangeHeartbeat task RangeDelete. range id: %" PRIu64,
                       resp.range_id());
            DeleteRange(resp.range_id());
            break;
        case taskpb::TaskType::RangeLeaderTransfer:
            FLOG_INFO("RangeHeartbeat task RangeLeaderTransfer. range id: %" PRIu64,
                       resp.range_id());
            // TODO
            // master undefinded
            break;
        case taskpb::TaskType::RangeAddPeer:
            FLOG_INFO("RangeHeartbeat task RangeAddPeer. range id: %" PRIu64,
                       resp.range_id());
            range->AddPeer(resp.task().range_add_peer().peer());
            break;
        case taskpb::TaskType::RangeDelPeer:
            FLOG_INFO("RangeHeartbeat task RangeDelPeer. range id: %" PRIu64,
                       resp.range_id());
            range->DelPeer(resp.task().range_del_peer().peer());
            break;
        default:
            FLOG_ERROR("RangeHeartbeat task error. range id: %" PRIu64 " task type: %d",
                       resp.range_id(), resp.task().type());
    }
}

void RangeServer::OnAskSplitResp(const mspb::AskSplitResponse &resp) {
    if (resp.has_header() && resp.header().has_error()) {
        FLOG_ERROR("AskSplit response has error range_id %" PRIu64, resp.range().id());
        return;
    }

    FLOG_INFO("range[%lu] recv AskSplit response from master.", resp.range().id());

    auto range = Find(resp.range().id());
    if (range == nullptr) {
        FLOG_ERROR("AdminSplit not found range_id %" PRIu64 " failed", resp.range().id());
        return;
    }

    // TODO: remove this copy
    auto copy_resp = resp;
    range->AdminSplit(copy_resp);
}

void RangeServer::CollectNodeHeartbeat(mspb::NodeHeartbeatRequest *req) {
    req->set_node_id(context_->node_id);

    auto stats = req->mutable_stats();
    stats->set_range_count(GetRangesSize());
    stats->set_range_leader_count(context_->run_status->GetLeaderCount());
    stats->set_range_split_count(context_->run_status->GetSplitCount());

    raft::ServerStatus rss;
    context_->raft_server->GetStatus(&rss);
    stats->set_sending_snap_count(rss.total_snap_sending);
    stats->set_receiving_snap_count(rss.total_snap_applying);
    stats->set_applying_snap_count(rss.total_snap_applying);

    // collect file system usage
    FileSystemUsage fs_usage;
    context_->run_status->GetFilesystemUsage(&fs_usage);
    stats->set_capacity(fs_usage.total_size);
    stats->set_used_size(fs_usage.used_size);
    stats->set_available(fs_usage.free_size);

    // collect storage metric
    storage::MetricStat mstat;
    storage::Metric::CollectAll(&mstat);
    stats->set_keys_read(mstat.keys_read_per_sec);
    stats->set_bytes_read(mstat.bytes_read_per_sec);
    stats->set_keys_written(mstat.keys_write_per_sec);
    stats->set_bytes_written(mstat.bytes_write_per_sec);

    stats->set_is_busy(false);
}

}  // namespace server
}  // namespace dataserver
}  // namespace sharkstore
