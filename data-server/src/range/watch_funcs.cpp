#include "range.h"
#include "server/range_server.h"
#include "watch.h"
#include "watch/watcher.h"

namespace sharkstore {
namespace dataserver {
namespace range {

Status Range::GetAndResp( const common::ProtoMessage *msg, watchpb::DsWatchRequest &req, std::string &key, std::string &val,
                          watchpb::DsWatchResponse *dsResp, uint64_t &version, errorpb::Error *err) {

    version = 0;
    Status ret = store_->Get(key, &val);

    auto resp = dsResp->mutable_resp();
    auto evt = resp->add_events();

    resp->set_watchid(msg->session_id);
    resp->set_code(static_cast<int>(ret.code()));

    auto userKv = new watchpb::WatchKeyValue;
    userKv->CopyFrom(req.req().kv());

    if(ret.ok()) {
        //decode value
        if(Status::kOk == WatchCode::DecodeKv(funcpb::kFuncWatchGet, meta_, userKv, key, val, err)) {
            evt->set_type(watchpb::PUT);
            evt->set_allocated_kv(userKv);
            version = userKv->version();

            FLOG_INFO("range[%" PRIu64 "] GetAndResp db_version: [%" PRIu64 "]", meta_.id(), version);
        } else {
            FLOG_WARN("range[%" PRIu64 "] GetAndResp db_version: [%" PRIu64 "]", meta_.id(), version);
            ret = Status(Status::kInvalid);
        }
    } else {
        //consume version returning to user
        version = getcurrVersion(err);
        evt->set_type(watchpb::DELETE);
        userKv->set_version(version);

        FLOG_INFO("range[%" PRIu64 "] GetAndResp code_: %s  version[%" PRId64 "]  key:%s", meta_.id(),
                  ret.ToString().c_str(), version, EncodeToHexString(key).c_str());
    }

    return ret;
}


void Range::WatchGet(common::ProtoMessage *msg, watchpb::DsWatchRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsWatchResponse;
    auto header = ds_resp->mutable_header();
    std::string dbKey{""};
    std::string dbValue{""};
    uint64_t dbVersion{0};

    //to do 暂不支持前缀watch
    auto prefix = req.req().prefix();
    auto tmpKv = req.req().kv();

    FLOG_DEBUG("range[%" PRIu64 "] WatchGet begin", meta_.id());

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        if( Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchGet, meta_, tmpKv, dbKey, dbValue, err) ) {
            break;
        }

        FLOG_DEBUG("range[%" PRIu64 " %s-%s] WatchGet key:%s", meta_.id(),  EncodeToHexString(meta_.start_key()).c_str(), EncodeToHexString(meta_.end_key()).c_str(), EncodeToHexString(dbKey).c_str());
        
        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(dbKey);
                break;
            }
        }

        //auto btime = get_micro_second();
        //get from rocksdb
        //auto ret = store_->Get(dbKey, &dbValue);
        //auto ret = GetAndResp(msg, req, dbKey, dbValue, ds_resp, dbVersion, err);
        //context_->run_status->PushTime(monitor::PrintTag::Store,
        //                               get_micro_second() - btime);


    } while (false);

    //int16_t watchFlag{0};

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] WatchGet error: %s", meta_.id(),
                  err->message().c_str());
        context_->socket_session->SetResponseHeader(req.header(), header, err);
        context_->socket_session->Send(msg, ds_resp);
        return;
    }

    //add watch if client version is not equal to ds side
    auto clientVersion = req.req().startversion();
    if(req.req().longpull() > 0) {
        uint64_t expireTime = get_micro_second();
        expireTime += (req.req().longpull()*1000);

        msg->expire_time = expireTime;
    }

    //to do add watch
    auto watch_server = context_->range_server->watch_server_;
    std::vector<watch::WatcherKey*> keys;

    for (auto i = 0; i < tmpKv.key_size(); i++) {
        keys.push_back(new watch::WatcherKey(tmpKv.key(i)));
    }

    auto w_ptr = std::make_shared<watch::Watcher>(meta_.table_id(), keys, clientVersion, msg);

    auto wcode = watch_server->AddKeyWatcher(w_ptr, store_);
    if(watch::WATCH_OK == wcode) {
        return;
    } else if(watch::WATCH_WATCHER_NOT_NEED == wcode) {
        auto btime = get_micro_second();
        //to do get from db again
        GetAndResp(msg, req, dbKey, dbValue, ds_resp, dbVersion, err);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
    } else {
        FLOG_ERROR("range[%" PRIu64 "] add watcher exception(%d).", meta_.id(), static_cast<int>(wcode));
    }
    //watch_server->WatchServerUnlock(1);

    w_ptr->Send(ds_resp);
    return;
}

void Range::PureGet(common::ProtoMessage *msg, watchpb::DsKvWatchGetMultiRequest &req) {
    errorpb::Error *err = nullptr;

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    auto ds_resp = new watchpb::DsKvWatchGetMultiResponse;
    auto header = ds_resp->mutable_header();
    //encode key and value
    std::string dbKey{""};
    std::string dbKeyEnd{""};
    std::string dbValue("");
    //int64_t version{0};
    int64_t minVersion(0);
    int64_t maxVersion(0);
    auto prefix = req.prefix();

    FLOG_DEBUG("range[%" PRIu64 "] PureGet begin", meta_.id());

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.kv().key();
        if (key.empty()) {
            FLOG_WARN("range[%" PRIu64 "] PureGet error: key empty", meta_.id());
            err = KeyNotInRange("EmptyKey");
            break;
        }

        //encode key
        if( 0 != WatchCode::EncodeKv(funcpb::kFuncWatchGet, meta_, req.kv(), dbKey, dbValue, err)) {
            break;
        }

        FLOG_WARN("range[%" PRIu64 "] PureGet key before:%s after:%s", meta_.id(), key[0].c_str(), EncodeToHexString(dbKey).c_str());

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(dbKey);
                break;
            }
        }

        auto resp = ds_resp;
        auto btime = get_micro_second();
        storage::Iterator *it = nullptr;
        Status::Code code = Status::kOk;

        if (prefix) {
            dbKeyEnd.assign(dbKey);
            if( 0 != WatchCode::NextComparableBytes(dbKey.data(), dbKey.length(), dbKeyEnd)) {
                //to do set error message
                break;
            }
            FLOG_DEBUG("range[%" PRIu64 "] PureGet key scope %s---%s", meta_.id(), EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbKeyEnd).c_str());

            //need to encode and decode
            std::shared_ptr<storage::Iterator> iterator(store_->NewIterator(dbKey, dbKeyEnd));
            uint32_t count{0};

            for (int i = 0; iterator->Valid() ; ++i) {
                count++;
                auto kv = resp->add_kvs();
                auto tmpDbKey = iterator.get()->key();
                auto tmpDbValue = iterator.get()->value();
                
                if(Status::kOk != WatchCode::DecodeKv(funcpb::kFuncPureGet, meta_, kv, tmpDbKey, tmpDbValue, err)) {
                    //break;
                    continue;
                    FLOG_DEBUG("range[%" PRIu64 "] dbvalue:%s  err:%s", meta_.id(), EncodeToHexString(tmpDbValue).c_str(), err->message().c_str());
                }
                //to judge version after decoding value and spliting version from value
                if (minVersion > kv->version()) {
                    minVersion = kv->version();
                }
                if(maxVersion < kv->version()) {
                    maxVersion = kv->version();
                }

                iterator->Next();
            }

            FLOG_DEBUG("range[%" PRIu64 "] PureGet ok:%d ", meta_.id(), count);
            code = Status::kOk;
        } else {
            auto kv = resp->add_kvs();
            auto ret = store_->Get(dbKey, &dbValue);

            if(ret.ok()) {
                //to do decode value version
                FLOG_DEBUG("range[%"
                                   PRIu64
                                   "] PureGet: dbKey:%s dbValue:%s  ", meta_.id(), EncodeToHexString(dbKey).c_str(),
                           EncodeToHexString(dbValue).c_str());
                if (Status::kOk != WatchCode::DecodeKv(funcpb::kFuncPureGet, meta_, kv, dbKey, dbValue, err)) {
                    FLOG_WARN("range[%"
                                       PRIu64
                                       "] DecodeKv fail. dbvalue:%s  err:%s", meta_.id(), EncodeToHexString(dbValue).c_str(),
                               err->message().c_str());
                    //break;
                }
            }

            FLOG_DEBUG("range[%" PRIu64 "] PureGet code:%d msg:%s userValue:%s ", meta_.id(), ret.code(), ret.ToString().data(), kv->value().c_str());
            code = ret.code();
        }
        context_->run_status->PushTime(monitor::PrintTag::Get,
                                       get_micro_second() - btime);

        resp->set_code(static_cast<int32_t>(code));
    } while (false);

    if (err != nullptr) {
        FLOG_WARN("range[%" PRIu64 "] PureGet error: %s", meta_.id(),
                  err->message().c_str());
    }

    context_->socket_session->SetResponseHeader(req.header(), header, err);
    context_->socket_session->Send(msg, ds_resp);
}

void Range::WatchPut(common::ProtoMessage *msg, watchpb::DsKvWatchPutRequest &req) {
    errorpb::Error *err = nullptr;
    std::string dbKey{""};
    //auto dbValue{std::make_shared<std::string>("")};
    //auto extPtr{std::make_shared<std::string>("")};

    auto btime = get_micro_second();
    context_->run_status->PushTime(monitor::PrintTag::Qwait, btime - msg->begin_time);

    FLOG_DEBUG("range[%" PRIu64 "] session_id:%" PRId64 " WatchPut begin", meta_.id(), msg->session_id);

    if (!CheckWriteable()) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        resp->mutable_resp()->set_code(Status::kNoLeftSpace);
        return SendError(msg, req.header(), resp, nullptr);
    }

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto kv = req.mutable_req()->mutable_kv();
        if (kv->key().empty()) {
            FLOG_WARN("range[%" PRIu64 "] WatchPut error: key empty", meta_.id());
            err = KeyNotInRange("-");
            break;
        }

        FLOG_DEBUG("range[%" PRIu64 "] WatchPut key:%s value:%s", meta_.id(), kv->key(0).c_str(), kv->value().c_str());

        /*
        //to do move to apply encode key
        if( 0 != version_seq_->nextId(&version)) {
            if (err == nullptr) {
                err = new errorpb::Error;
            }
            err->set_message(version_seq_->getErrMsg());
            break;
        }
        kv->set_version(version);
        FLOG_DEBUG("range[%" PRIu64 "] WatchPut key-version[%" PRIu64 "]", meta_.id(), version);

        if( Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchPut, meta_, *kv, *dbKey, *dbValue, err) ) {
            break;
        }*/

        std::vector<std::string*> vecUserKeys;
        for ( auto i = 0 ; i < kv->key_size(); i++) {
            vecUserKeys.emplace_back(kv->mutable_key(i));
        }

        watch::Watcher::EncodeKey(&dbKey, meta_.table_id(), vecUserKeys);

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(dbKey);
            } else {
                err = StaleEpochError(epoch);
            }

            break;
        }

        /*
        //increase key version
        kv->set_version(version);
        kv->clear_key();
        kv->add_key(*dbKey);
        kv->set_value(*dbValue);
        */

        //raft propagate at first, propagate KV after encodding
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
    std::string dbKey{""};
    //auto dbValue = std::make_shared<std::string>();
    //auto extPtr = std::make_shared<std::string>();

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

        auto kv = req.mutable_req()->mutable_kv();

        if (kv->key_size() < 1) {
            FLOG_WARN("range[%" PRIu64 "] WatchDel error due to key is empty", meta_.id());
            err = KeyNotInRange("EmptyKey");
            break;
        }

        /*
        if(Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchDel, meta_, *kv, *dbKey, *dbValue, err)) {
            break;
        }*/

        /*std::vector<std::string*> vecUserKeys;
        for(auto itKey : kv->key()) {
            vecUserKeys.emplace_back(&itKey);
        }*/
        std::vector<std::string*> vecUserKeys;
        for ( auto i = 0 ; i < kv->key_size(); i++) {
            vecUserKeys.emplace_back(kv->mutable_key(i));
        }

        watch::Watcher::EncodeKey(&dbKey, meta_.table_id(), vecUserKeys);

        auto epoch = req.header().range_epoch();
        bool in_range = KeyInRange(dbKey);
        bool is_equal = EpochIsEqual(epoch);

        if (!in_range) {
            if (is_equal) {
                err = KeyNotInRange(dbKey);
            } else {
                err = StaleEpochError(epoch);
            }
            break;
        }
        /*
        //set encoding value to request
        kv->clear_key();
        kv->add_key(*dbKey);
        kv->set_value(*dbValue);
        */

        /*to do move to apply
        //to do consume version and will reply to client
        int64_t version{0};
        if( 0 != version_seq_->nextId(&version)) {
            if (err == nullptr) {
                err = new errorpb::Error;
            }
            err->set_message(version_seq_->getErrMsg());
            break;
        }
        kv->set_version(version);
        */

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
    auto &kv = req.req().kv();

    if (is_leader_ && kv.key_size() > 0 ) {
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
    auto &kv = req.req().kv();

    if (is_leader_ && kv.key_size() > 0 ) {
        auto ret = SubmitCmd(msg, req, [&req](raft_cmdpb::Command &cmd) {
            cmd.set_cmd_type(raft_cmdpb::CmdType::KvWatchDel);
            cmd.set_allocated_kv_watch_del_req(req.release_req());
        });

        return ret.ok() ? true : false;
    }

    return false;
}

Status Range::ApplyWatchPut(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    FLOG_DEBUG("range [%" PRIu64 "]ApplyWatchPut begin", meta_.id());
    auto &req = cmd.kv_watch_put_req();
    watchpb::WatchKeyValue notifyKv;
    notifyKv.CopyFrom(req.kv());

    int64_t version{0};
    version = getNextVersion(err);
    notifyKv.set_version(version);
    FLOG_DEBUG("range[%" PRIu64 "] ApplyWatchPut new version[%" PRIu64 "]", meta_.id(), version);

    std::string dbKey{""};
    std::string dbValue{""};
    if( Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchPut, meta_, notifyKv, dbKey, dbValue, err) ) {
        //to do
        // SendError()
        FLOG_WARN("EncodeKv failed, key:%s ", notifyKv.key(0).c_str());
        ;
    }

    notifyKv.clear_key();
    notifyKv.add_key(dbKey);
    notifyKv.set_value(dbValue);
    FLOG_DEBUG("ApplyWatchPut range[%" PRIu64 "] dbkey:%s dbvalue:%s", meta_.id(), EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbValue).c_str());

    do {

        if (!KeyInRange(dbKey, err)) {
            FLOG_WARN("Apply WatchPut failed, key:%s not in range.", dbKey.data());
            ret = std::move(Status(Status::kInvalidArgument, "key not in range", ""));
            break;
        }

        //save to db
        auto btime = get_micro_second();
        ret = store_->Put(dbKey, dbValue);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyWatchPut failed, code:%d, msg:%s", ret.code(),
                       ret.ToString().data());
            break;
        }

        if (cmd.cmd_id().node_id() == node_id_) {
            auto len = static_cast<uint64_t>(req.kv().ByteSizeLong());
            CheckSplit(len);
        }
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchPutResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
        return ret;
    }

    //notify watcher
    std::string errMsg("");
    int32_t retCnt = WatchNotify(watchpb::PUT, notifyKv, errMsg);
    if (retCnt < 0) {
        FLOG_ERROR("WatchNotify-put failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
    } else {
        FLOG_DEBUG("WatchNotify-put success, count:%d, msg:%s", retCnt, errMsg.c_str());
    }
    
    return ret;
}

Status Range::ApplyWatchDel(const raft_cmdpb::Command &cmd) {
    Status ret;
    errorpb::Error *err = nullptr;

    FLOG_DEBUG("range[%"
                       PRIu64
                       "] ApplyWatchDel begin", meta_.id());

    auto &req = cmd.kv_watch_del_req();
    watchpb::WatchKeyValue notifyKv;
    notifyKv.CopyFrom(req.kv());

    int64_t version{0};
    version = getNextVersion(err);
    notifyKv.set_version(version);
    FLOG_DEBUG("range[%"
                       PRIu64
                       "] ApplyWatchDel new-version[%"
                       PRIu64
                       "]", meta_.id(), version);


    std::string dbKey{""};
    std::string dbValue{""};
    if (Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchDel, meta_, notifyKv, dbKey, dbValue, err)) {
        //to do response error
        //SendError()
        FLOG_WARN("EncodeKv failed, key:%s ", notifyKv.key(0).c_str());
        ;
    }

    notifyKv.clear_key();
    notifyKv.add_key(dbKey);
    if (!dbValue.empty()) {
        notifyKv.set_value(dbValue);
    }


    do {
        if (!KeyInRange(dbKey, err)) {
            FLOG_WARN("ApplyWatchDel failed, key:%s not in range.", dbKey.data());
            break;
        }

        //auto watch_server = context_->range_server->watch_server_;
        auto btime = get_micro_second();

        //auto wSet = watch_server->GetWatcherSet_(dbKey);
        //wSet->WatchSetLock(1);
        ret = store_->Delete(dbKey);
        //wSet->WatchSetLock(0);

        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);

        if (!ret.ok()) {
            FLOG_ERROR("ApplyWatchDel failed, code:%d, msg:%s , key:%s", ret.code(),
                       ret.ToString().c_str(), EncodeToHexString(dbKey).c_str());
            break;
        }
        // ignore delete CheckSplit
    } while (false);

    if (cmd.cmd_id().node_id() == node_id_) {
        auto resp = new watchpb::DsKvWatchDeleteResponse;
        SendResponse(resp, cmd, static_cast<int>(ret.code()), err);
    } else if (err != nullptr) {
        delete err;
        return ret;
    }

    FLOG_DEBUG("store->Delete->ret.code:%s", ret.ToString().c_str());
    /*if( ret.code() == Status::kNotFound ) {
        return ret;
    }*/

    //notify watcher
    int32_t retCnt(0);
    std::string errMsg("");
    retCnt = WatchNotify(watchpb::DELETE, notifyKv, errMsg);
    if (retCnt < 0) {
        FLOG_ERROR("WatchNotify-del failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
    } else {
        FLOG_DEBUG("WatchNotify-del success, count:%d, msg:%s", retCnt, errMsg.c_str());
    }
    
    return ret;
}

int32_t Range::WatchNotify(const watchpb::EventType evtType, const watchpb::WatchKeyValue& kv, std::string &errMsg) {
//    Status ret;
    int32_t idx{0};

    std::shared_ptr<watchpb::WatchKeyValue> tmpKv = std::make_shared<watchpb::WatchKeyValue>();
    tmpKv->CopyFrom(kv);

    std::vector<watch::WatcherPtr> vecNotifyWatcher;
    auto dbKey = tmpKv->key_size()>0?tmpKv->key(0):"NOFOUND";
    if(dbKey == "NOFOUND") {
        errMsg.assign("WatchNotify--key is empty.");
        return -1;
    }

    decltype(dbKey) dbPreKey{""};
    auto dbValue = tmpKv->value();
    auto currDbVersion = tmpKv->version();

    //std::string key{""};
    //std::string value{""};
    errorpb::Error *err = new errorpb::Error;

    auto watch_server = context_->range_server->watch_server_;
    watch_server->GetKeyWatchers(vecNotifyWatcher, dbKey, currDbVersion);

    //continue to get prefix key
    std::vector<std::string *> decodeKeys;
    if(watch::Watcher::DecodeKey(decodeKeys, dbKey)) {

        if (decodeKeys.size() > 1) {
            decodeKeys.erase(decodeKeys.end() - 1);

            watch::Watcher::EncodeKey(&dbPreKey, meta_.id(), decodeKeys);

            std::vector<watch::WatcherPtr> vecPrefixWatcher;
            watch_server->GetPrefixWatchers(vecPrefixWatcher, dbPreKey, currDbVersion);

            auto cnt = 0L;
            for( auto it : vecPrefixWatcher) {
                vecNotifyWatcher.push_back( it );
                cnt ++;
            }
            FLOG_DEBUG("encode prefix key:%s  watcher count:%ld", EncodeToHexString(dbPreKey).c_str(), cnt);
        }
    } else {
        FLOG_WARN("DecodeKey error when WatchNotify, key:%s", EncodeToHexString(dbKey).c_str());
    }

    auto watchCnt = vecNotifyWatcher.size();
    if (watchCnt > 0) {
        //to do 遍历watcher 发送通知

        //    evt->set_allocated_kv(tmpKv.get());
        funcpb::FunctionID funcId;

        if (watchpb::DELETE == evtType) {
            funcId = funcpb::kFuncWatchDel;
        } else {
            funcId = funcpb::kFuncWatchPut;
        }

        for(auto w: vecNotifyWatcher) {
            auto w_id = w->GetWatcherId();
            idx++;
            FLOG_DEBUG("range[%" PRIu64 "] session_id:%" PRId64 " Watch-Notify(%d)[key][%s] (%" PRId32"/%" PRIu32")>>>[watch_id][%" PRId64"]",
                       meta_.id(), w->getSessionId(), funcId, dbKey.c_str(), idx, uint32_t(watchCnt), w_id);

            assert(w_id > );
            /*if(w_id < 1) {
                FLOG_ERROR("range[%" PRIu64 "] WatchNotify warn: watch_id is invalid.", meta_.id());
                free(w->GetMessage());
                continue;
            }*/

            auto ds_resp = new watchpb::DsWatchResponse;
            auto resp = ds_resp->mutable_resp();
            resp->set_code(Status::kOk);

            auto evt = resp->add_events();

            auto decodeKv = new watchpb::WatchKeyValue;
            decodeKv->CopyFrom(*(tmpKv.get()));

            if( Status::kOk != WatchCode::DecodeKv(funcId, meta_, decodeKv, dbKey, dbValue, err)) {
                errMsg.assign("WatchNotify--Decode key:");
                errMsg.append(dbKey);
                errMsg.append(" fail.");
                //free(w->GetMessage());

                resp->set_watchid(w_id);
                resp->set_code(Status::kUnexpected);
                w->Send(resp);

                return -1;
            }

            evt->set_allocated_kv(decodeKv);
            evt->set_type(evtType);

            resp->set_watchid(w_id);

            w->Send(ds_resp);
            {
                //delete watch
                watch::WatchCode del_ret;
                if (w->GetType() == watch::WATCH_KEY) {
                    del_ret = watch_server->DelKeyWatcher(w);
                } else {
                    del_ret = watch_server->DelPrefixWatcher(w);
                }
                if (del_ret) {
                    FLOG_WARN("range[%" PRIu64 "] Watch-Notify DelWatcher WARN:[key][%s] (%" PRId32"/%" PRIu32")>>>[session][%" PRId64"]",
                           meta_.id(), EncodeToHexString(dbKey).c_str(), idx, uint32_t(watchCnt), w_id);
                } else {
                    FLOG_DEBUG("range[%" PRIu64 "] DelWatcher success. key:%s session_id:%" PRIu64 "...", meta_.id(), EncodeToHexString(dbKey).c_str(), w_id);
                }
            }
        }
    } else {
        idx = 0;
        errMsg.assign("no watcher");
        FLOG_WARN("range[%" PRIu64 "] Watch-Notify key:%s has no watcher.",
                           meta_.id(), EncodeToHexString(dbKey).c_str());

    }

    return idx;

}

}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
