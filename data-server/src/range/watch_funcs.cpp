#include "range.h"

#include "server/range_server.h"
#include "watch.hpp"

namespace sharkstore {
namespace dataserver {
namespace range {

watchpb::DsWatchResponse *Range::WatchGetResp(const std::string &key) {
    if (is_leader_ && KeyInRange(key)) {
        auto resp = new watchpb::DsWatchResponse;
        auto ret = store_->Get(key, resp->mutable_value());
        if (ret.ok()) {
            resp->set_code(0);
        } else {
            resp->set_code(static_cast<int>(ret.code()));
        }
        return resp;
    }

    return nullptr;
}

void Range::WatchGet(common::ProtoMessage *msg, watchpb::DsWatchRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsWatchResponse;
    auto header = ds_resp->mutable_header();

    FLOG_DEBUG("range[%" PRIu64 "] WatchGet begin", meta_.id());

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().key();
        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] WatchGet error: key empty", meta_.id());
            err = KeyNotInRange(key);
            break;
        }
        FLOG_DEBUG("range[%"PRIu64" %s-%s] WatchGet key:%s", meta_.id(), meta_.start_key().c_str(), meta_.end_key().c_str(), key.c_str());
        
        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(key);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(key);
                break;
            }

//        //! is_equal then retry watch get
//        auto resp = WatchGetTry(key);
//        if (resp != nullptr) {
//            ds_resp->set_allocated_resp(resp);
//        } else {
//            err = StaleEpochError(epoch);
//        }
//          FLOG_WARN("range[%" PRIu64 "] WatchGet error: Key '%s' is not in range", meta_.id(), key.c_str());
//          break;
        }

        auto resp = ds_resp->mutable_resp();
        auto evt = resp->mutable_events()->add_events();
        auto kv = evt->mutable_kv();

        auto btime = get_micro_second();
        //get from rocksdb
        auto ret = store_->Get(req.req().key(), kv->mutable_value());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        evt->set_type(PUT);
        evt->set_WatchId(msg->session_id);
        evt->set_code(Status::kOk);
        kv->set_key(key);
        
        resp->set_code(static_cast<int>(ret.code()));
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] WatchGet error: %s", meta_.id(),
                  err->message().c_str());
    } else {

        //add watch if client version is not equal to ds side
        auto &start_version = req.req().StartVersion();
        //to do 暂不支持前缀watch
        auto prefix = req.req().prefix();

        //decode version from value
        auto ds_version = 0;
        for (auto evt : ds_resp->resp().events()) {
            decodeVersion(evt.kv().value(), &ds_version);
        
            if(start_version >= ds_version) {
                //to do add watch
                AddKeyWatcher(evt.kv().key(), msg);
            } else {
                FLOG_DEBUG("range[%" PRIu64 "] WatchGet [%s]-%s ok.", meta_.id(), req.req().key().data(), evt.kv().value().data());
            }
        }
    }

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

void Range::PureGet(common::ProtoMessage *msg, watchpb::DsKvWatchGetMultiRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsWatchResponse;
    auto header = ds_resp->mutable_header();

    FLOG_DEBUG("range[%" PRIu64 "] PureGet begin", meta_.id());

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().kv().key();
        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] PureGet error: key empty", meta_.id());
            err = KeyNotInRange(key);
            break;
        }
        auto prefix = req.req().prefix();

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(key);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(key);
                break;
            }

//          //! is_equal then retry watch get
//          auto resp = WatchGetTry(key);
//          if (resp != nullptr) {
//              ds_resp->set_allocated_resp(resp);
//          } else {
//              err = StaleEpochError(epoch);
//          }
//
//          break;
        }

        auto resp = ds_resp->mutable_resp();
        auto btime = get_micro_second();
        Iterator *it = nullptr;
        Status::Code code;

        if (prefix) {
            //need to encode and decode
            std::unique_ptr<storage::Iterator> iterator(
                store_->NewIterator(req.req().kv().key(), req.req().kv().key()));
            uint32_t count{0};

            for (int i = 0; iterator->Valid() ; ++i) {
                auto evt = resp->mutable_events()->add_events();
                auto kv = evt->mutable_kv();

                kv->set_key((std::move(iterator->key()));
                kv->set_value(std::move(iterator->value()));
                //to do decode value version 
                iterator->Next();
                count++;
            }

            FLOG_DEBUG("range[%" PRIu64 "] PureGet ok:%d ", meta_.id(), count);
            code = kOk;
        } else {
            auto evt = resp->mutable_events()->add_events();
            auto ret = store_->Get(req.req().kv().key(), evt->mutable_kv()->mutable_value());
            //to do decode value version 
            FLOG_DEBUG("range[%" PRIu64 "] PureGet code:%d msg:%s ", meta_.id(), code, ret.ToString().data());
            code = ret.code();
        }
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        resp->set_code(static_cast<int>(code));
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] PureGet error: %s", meta_.id(),
                  err->message().c_str());
    }

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

watchpb::DsWatchResponse *Range::WatchGetTry(const std::string &key) {
    auto rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return nullptr;
    }

    return rng->WatchGetResp(key);
}

void Range::WatchPut(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    //TO DO
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] WatchPut begin", meta_.id());

    if (!CheckWriteable()) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().kv().key();
        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] WatchPut error: key empty", meta_.id());
            err = KeyNotInRange(key);
            break;
        }

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(key);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(key);
                break;
            }

//          //! is_equal then retry watch put
//          if (!WatchPutTry(msg, req)) {
//              err = StaleEpochError(epoch);
//          }
//          FLOG_WARN("range[%" PRIu64 "] WatchPut error: key is not in range", meta_.id());
//          break;
        }
        
        //increase key version
        uint64_t version_seq = version_seq_.fetch_add(1);
        req.mutable_req()->mutable_kv()->set_version(version_seq);

        //to do set key&value and propagate to peers
        //...
        
        //raft propagate at first 
        if (!WatchPutSubmit(msg, req)) {
            err = RaftFailError();

        }
        
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] WatchPut error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new watchpb::DsKvWatchPutResponse;
        return SendError(msg, req.header(), resp, err);
    }

}

void Range::WatchDel(common::ProtoMessage *msg, watchpb::DsKvWatchDeleteRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] WatchDel begin", meta_.id());

    if (!CheckWriteable()) {
        auto resp = new watchpb::DsKvWatchDeleteResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.req().kv().key();

        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] WatchDel error: key empty", meta_.id());
            err = KeyNotInRange(key);
            break;
        }

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(key);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(key);
                break;
            }

//          //! is_equal then retry watch delete
//          if (!WatchDelTry(msg, req)) {
//              err = StaleEpochError(epoch);
//          }
//
//          break;
        }

        if (!WatchDeleteSubmit(msg, req)) {
            err = RaftFailError();
        }
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] WatchDel error: %s", meta_.id(),
                  err->message().c_str());

        auto resp = new watchpb::DsKvWatchDeleteResponse;
        return SendError(msg, req.header(), resp, err);
    }
    
}

bool Range::WatchPutSubmit(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && KeyInRange(key)) {
        
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvWatchPut);
            cmd.set_allocated_kv_watch_put_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

bool Range::WatchDeleteSubmit(common::ProtoMessage *msg,
                            watchpb::DsKvWatchDeleteRequest &req) {
    auto &key = req.req().key();

    if (is_leader_ && KeyInRange(key)) {
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvWatchDel);
            cmd.set_allocated_kv_watch_del_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

bool Range::WatchPutTry(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    auto rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return false;
    }

    return rng->WatchPutSubmit(msg, req);
}

bool Range::WatchDelTry(common::ProtoMessage *msg, watchpb::DsKvWatchDeleteRequest &req) {
    std::shared_ptr<Range> rng = context_->range_server->find(split_range_id_);
    if (rng == nullptr) {
        return false;
    }

    return rng->WatchDeleteSubmit(msg, req);
}

Status Range::ApplyWatchPut(const raft_cmdpb::Command &cmd) {
    Status ret;

    FLOG_DEBUG("range [%" PRIu64 "]ApplyWatchPut begin", meta_.id());
    auto &req = cmd.kv_watch_put_req();

    errorpb::Error *err = nullptr;

    do {
        if (!KeyInRange(req.key(), err)) {
            FLOG_WARN("Apply WatchPut failed, epoch is changed");
            ret = std::move(Status(Status::kInvalidArgument, "key not int range", ""));
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Put(req.key(), req.value());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyWatchPut failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().data());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = req.key().size() + req.value().size();
            CheckSplit(len);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
    }

    //notify watcher
    //uint64_t version_seq = version_seq_.fetch_add(1);
    auto &value = req.req().kv().mutable_value();

    //strip version and tableId from value
    //WatchEncodeValue(&value, version_seq);
    ret = WatchNotify(PUT, req.req().kv());
    if (!ret.ok()) {
        FLOG_ERROR("WatchNotify failed, code:%d, msg:%s", ret.code(), ret.ToString().c_str());
    }
    
    return ret;
}

Status Range::ApplyWatchDel(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    FLOG_DEBUG("range[%" PRIu64 "] ApplyWatchDel begin", meta_.id());

    auto &req = cmd.kv_watch_del_req();

    do {
        if (!KeyInRange(req.key(), err)) {
            FLOG_WARN("ApplyWatchDel failed, epoch is changed");
            break;
        }

        auto btime = get_micro_second();
        ret = store_->Delete(req.key());
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyWatchDel failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().c_str());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchDeleteResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
    }

    //notify watcher
    uint64_t version_seq(0);
    auto &value = req.req().kv().value();
    
    //strip version from value
    //to do 
    ret = WatchNotify(DELETE, req.req().kv());
    if (!ret.ok()) {
        FLOG_ERROR("WatchNotify failed, code:%d, msg:%s", ret.code(), ret.ToString().c_str());
    }
    
    return ret;
}

Status Range::WatchNotify(const EventType evtType, const watchpb::WatchKeyValue& kv) {
    Status ret;

    std::vector<common::ProtoMessage*> vecProtoMsg;
    auto &key = req.req().kv().key();
    auto &value = req.req().kv().value();

    uint32_t watchCnt = GetKeyWatchers(vecProtoMsg, key);
    if (watchCnt > 0) {
        //to do 遍历watcher 发送通知
        auto ds_resp = new watchpb::DsWatchResponse;
        auto resp = ds_resp->mutable_resp();
        resp->set_code(kOk);
        resp->set_watchId(msg->session_id);

        auto evt = resp->mutable_events()->add_events();
        evt->set_type(evtType);
        evt->mutable_kv()->set_key(key);
        evt->mutable_kv()->set_value(value);

        int32_t idx{0};
        for(auto &pMsg : vecProtoMsg) {
            idx++;
            FLOG_DEBUG("range[%" PRIu64 "] WatchPut-Notify[key][%s] (%d/%d)>>>[session][%lld]", 
                       meta_.id(), key, idx, watchCnt, pMsg->session_id);
            if(0 != pMsg->socket->Send(ds_resp)) {
                FLOG_ERROR("range[%" PRIu64 "] WatchPut-Notify error:[key][%s] (%d/%d)>>>[session][%lld]", 
                           meta_.id(), key, idx, watchCnt, pMsg->session_id);
            } else {
                //delete watch
                if (WATCH_OK != DelKeyWatcher(pMsg->session_id, key)) {
                    FLOG_WARN("range[%" PRIu64 "] WatchPut-Notify DelKeyWatcher WARN:[key][%s] (%d/%d)>>>[session][%lld]", 
                           meta_.id(), key, idx, watchCnt, pMsg->session_id);
                }
            }
            
        }
    }

    return ret;

}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
