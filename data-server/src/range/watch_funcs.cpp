#include "range.h"
#include "server/range_server.h"
#include "watch.h"
#include "watch/watcher.h"

#include "range_logger.h"

namespace sharkstore {
namespace dataserver {
namespace range {

Status Range::GetAndResp( const common::ProtoMessage *msg, watchpb::DsWatchRequest &req, std::string &key, std::string &val,
                          watchpb::DsWatchResponse *dsResp, uint64_t &version, const bool &prefix) {

    version = 0;
    Status ret = store_->Get(key, &val);

    auto resp = dsResp->mutable_resp();
    auto evt = resp->add_events();

    resp->set_watchid(msg->session_id);
    resp->set_code(static_cast<int>(ret.code()));

    auto userKv = new watchpb::WatchKeyValue;
    userKv->CopyFrom(req.req().kv());

    auto err = std::make_shared<errorpb::Error>();
    if(ret.ok()) {
        //decode value
        if(Status::kOk == WatchCode::DecodeKv(funcpb::kFuncWatchGet, meta_.Get(), userKv, key, val, err.get())) {
            evt->set_type(watchpb::PUT);
            evt->set_allocated_kv(userKv);
            version = userKv->version();

            RANGE_LOG_INFO("GetAndResp db_version: [%" PRIu64 "]", version);
        } else {
            RANGE_LOG_WARN("GetAndResp db_version: [%" PRIu64 "]", version);
            ret = Status(Status::kInvalid);
        }
    } else {
        //consume version returning to user
        //version = getcurrVersion(err.get());
        //to do get raft index
        evt->set_type(watchpb::DELETE);
        userKv->set_version(version);

        RANGE_LOG_INFO("GetAndResp code_: %s  version[%" PRId64 "]  key:%s",
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

    RANGE_LOG_DEBUG("WatchGet begin");

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        if( Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchGet, meta_.Get(), tmpKv, dbKey, dbValue, err) ) {
            break;
        }

        FLOG_DEBUG("range[%" PRIu64 " %s-%s] WatchGet key:%s", id_,  EncodeToHexString(meta_.GetStartKey()).c_str(),
                EncodeToHexString(meta_.GetEndKey()).c_str(), EncodeToHexString(dbKey).c_str());
        
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
        RANGE_LOG_WARN("WatchGet error: %s", err->message().c_str());
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

    auto w_ptr = std::make_shared<watch::Watcher>(meta_.GetTableID(), keys, clientVersion, msg);

    watch::WatchCode wcode;
    if(prefix) {
        wcode = watch_server->AddPrefixWatcher(w_ptr, store_);
    } else {
        wcode = watch_server->AddKeyWatcher(w_ptr, store_);
    }

    if(watch::WATCH_OK == wcode) {
        return;
    } else if(watch::WATCH_WATCHER_NOT_NEED == wcode) {
        auto btime = get_micro_second();
        //to do get from db again
        GetAndResp(msg, req, dbKey, dbValue, ds_resp, dbVersion, prefix);
        context_->run_status->PushTime(monitor::PrintTag::Store,
                                       get_micro_second() - btime);
    } else {
        RANGE_LOG_ERROR("add watcher exception(%d).", static_cast<int>(wcode));
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

    RANGE_LOG_DEBUG("PureGet begin");

    do {
        if (!VerifyLeader(err)) {
            break;
        }

        auto &key = req.kv().key();
        if (key.empty()) {
            RANGE_LOG_WARN("PureGet error: key empty");
            err = KeyNotInRange("EmptyKey");
            break;
        }

        //encode key
        if( 0 != WatchCode::EncodeKv(funcpb::kFuncWatchGet, meta_.Get(), req.kv(), dbKey, dbValue, err)) {
            break;
        }

        RANGE_LOG_WARN("PureGet key before:%s after:%s", key[0].c_str(), EncodeToHexString(dbKey).c_str());

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
            RANGE_LOG_DEBUG("PureGet key scope %s---%s", EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbKeyEnd).c_str());

            //need to encode and decode
            std::shared_ptr<storage::Iterator> iterator(store_->NewIterator(dbKey, dbKeyEnd));
            uint32_t count{0};

            for (int i = 0; iterator->Valid() ; ++i) {
                count++;
                auto kv = resp->add_kvs();
                auto tmpDbKey = iterator.get()->key();
                auto tmpDbValue = iterator.get()->value();

                if(Status::kOk != WatchCode::DecodeKv(funcpb::kFuncPureGet, meta_.Get(), kv, tmpDbKey, tmpDbValue, err)) {
                    //break;
                    continue;
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

            RANGE_LOG_DEBUG("PureGet ok:%d ", count);
            code = Status::kOk;
        } else {
            auto kv = resp->add_kvs();
            auto ret = store_->Get(dbKey, &dbValue);

            if(ret.ok()) {
                //to do decode value version
                RANGE_LOG_DEBUG("PureGet: dbKey:%s dbValue:%s  ", EncodeToHexString(dbKey).c_str(),
                           EncodeToHexString(dbValue).c_str());
                if (Status::kOk != WatchCode::DecodeKv(funcpb::kFuncPureGet, meta_.Get(), kv, dbKey, dbValue, err)) {
                    RANGE_LOG_WARN("DecodeKv fail. dbvalue:%s  err:%s", EncodeToHexString(dbValue).c_str(),
                               err->message().c_str());
                    //break;
                }
            }

            RANGE_LOG_DEBUG("PureGet code:%d msg:%s userValue:%s ", ret.code(), ret.ToString().data(), kv->value().c_str());
            code = ret.code();
        }
        context_->run_status->PushTime(monitor::PrintTag::Get,
                                       get_micro_second() - btime);

        resp->set_code(static_cast<int32_t>(code));
    } while (false);

    if (err != nullptr) {
        RANGE_LOG_WARN("PureGet error: %s", err->message().c_str());
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

    RANGE_LOG_DEBUG("session_id:%" PRId64 " WatchPut begin", msg->session_id);

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
            RANGE_LOG_WARN("WatchPut error: key empty");
            err = KeyNotInRange("-");
            break;
        }

        RANGE_LOG_DEBUG("WatchPut key:%s value:%s", kv->key(0).c_str(), kv->value().c_str());

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

        watch::Watcher::EncodeKey(&dbKey, meta_.GetTableID(), vecUserKeys);

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
        RANGE_LOG_WARN("WatchPut error: %s", err->message().c_str());

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

    RANGE_LOG_DEBUG("WatchDel begin");

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
            RANGE_LOG_WARN("WatchDel error due to key is empty");
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

        watch::Watcher::EncodeKey(&dbKey, meta_.GetTableID(), vecUserKeys);

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
        RANGE_LOG_WARN("WatchDel error: %s", err->message().c_str());

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

Status Range::ApplyWatchPut(const raft_cmdpb::Command &cmd, uint64_t raftIdx) {
    Status ret;
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyWatchPut begin");
    auto &req = cmd.kv_watch_put_req();
    watchpb::WatchKeyValue notifyKv;
    notifyKv.CopyFrom(req.kv());

    int64_t version{0};
    //version = getNextVersion(err);
    version = raftIdx;
    notifyKv.set_version(version);
    RANGE_LOG_DEBUG("ApplyWatchPut new version[%" PRIu64 "]", version);

    std::string dbKey{""};
    std::string dbValue{""};
    if( Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchPut, meta_.Get(), notifyKv, dbKey, dbValue, err) ) {
        //to do
        // SendError()
        FLOG_WARN("EncodeKv failed, key:%s ", notifyKv.key(0).c_str());
        ;
    }

    notifyKv.clear_key();
    notifyKv.add_key(dbKey);
    notifyKv.set_value(dbValue);
    RANGE_LOG_DEBUG("ApplyWatchPut dbkey:%s dbvalue:%s", EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbValue).c_str());

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

        if(req.kv().key_size() > 1) {
            //to do decode group key,ignore single key
            /*std::vector<std::string*> keys;
            std::string dbPreKey{""};

            for(auto i =0; i < req.kv().key_size()-1; i++ ) {
                keys.emplace_back(std::move(new std::string(req.kv().key(i))));
            }
            watch::Watcher::EncodeKey(&dbPreKey, meta_.GetTableID(), keys);
            */
            auto value = std::make_shared<watch::CEventBufferValue>(notifyKv, watchpb::PUT);
            if(value->key_size()) {
                FLOG_DEBUG(">>>key is valid.");
            }

            if (!eventBuffer->enQueue(dbKey, value.get())) {
                FLOG_ERROR("load delete event kv to buffer error.");
            }
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
    int32_t retCnt = WatchNotify(watchpb::PUT, req.kv(), version, errMsg);
    if (retCnt < 0) {
        FLOG_ERROR("WatchNotify-put failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
    } else {
        FLOG_DEBUG("WatchNotify-put success, count:%d, msg:%s", retCnt, errMsg.c_str());
    }

    return ret;
}

Status Range::ApplyWatchDel(const raft_cmdpb::Command &cmd, uint64_t raftIdx) {
    Status ret;
    errorpb::Error *err = nullptr;

    RANGE_LOG_DEBUG("ApplyWatchDel begin");

    auto &req = cmd.kv_watch_del_req();
    watchpb::WatchKeyValue notifyKv;
    notifyKv.CopyFrom(req.kv());

    uint64_t version{0};
    //version = getNextVersion(err);
    version = raftIdx;
    notifyKv.set_version(version);
    RANGE_LOG_DEBUG("ApplyWatchDel new-version[%" PRIu64 "]", version);


    std::string dbKey{""};
    std::string dbValue{""};
    if (Status::kOk != WatchCode::EncodeKv(funcpb::kFuncWatchDel, meta_.Get(), notifyKv, dbKey, dbValue, err)) {
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

        if(req.kv().key_size() > 1) {
            //to do decode group key,ignore single key
            /*std::vector<std::string*> keys;
            std::string dbPreKey{""};

            for(auto i =0; i < req.kv().key_size()-1; i++ ) {
                keys.emplace_back(std::move(new std::string(req.kv().key(i))));
            }
            watch::Watcher::EncodeKey(&dbPreKey, meta_.GetTableID(), keys);
            */
            auto value = std::make_shared<watch::CEventBufferValue>(notifyKv, watchpb::DELETE);
            if(value->key_size()) {
                FLOG_DEBUG(">>>key is valid.");
            }
            if (!eventBuffer->enQueue(dbKey, value.get())) {
                FLOG_ERROR("load delete event kv to buffer error.");
            }
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
    retCnt = WatchNotify(watchpb::DELETE, req.kv(), version, errMsg);
    if (retCnt < 0) {
        FLOG_ERROR("WatchNotify-del failed, ret:%d, msg:%s", retCnt, errMsg.c_str());
    } else {
        FLOG_DEBUG("WatchNotify-del success, count:%d, msg:%s", retCnt, errMsg.c_str());
    }

    return ret;
}

int32_t Range::WatchNotify(const watchpb::EventType evtType, const watchpb::WatchKeyValue& kv, const int64_t &version, std::string &errMsg) {
//    Status ret;
    int32_t ret{0};
    bool groupFlag{false};

    //user kv
    //std::shared_ptr<watchpb::WatchKeyValue> tmpKv = std::make_shared<watchpb::WatchKeyValue>(kv);

    std::vector<watch::WatcherPtr> vecNotifyWatcher;
    std::vector<watch::WatcherPtr> vecPrefixNotifyWatcher;

    auto userKey = kv.key_size()>0?kv.key(0):"__NOFOUND__";
    if(userKey == "__NOFOUND__") {
        errMsg.assign("WatchNotify--key is empty.");
        return -1;
    }

    //continue to get prefix key
    std::vector<std::string *> decodeKeys;
    std::string hashKey{""};
    std::string dbKey{""};
    bool hasPrefix{false};
    if(kv.key_size() > 1) {
        hasPrefix = true;
    }

    for(auto it : kv.key()) {
        decodeKeys.emplace_back(std::move(new std::string(it)));
        //only push the first key
        break;
    }

    watch::Watcher::EncodeKey(&hashKey, meta_.GetTableID(), decodeKeys);
    if(hasPrefix) {
        int16_t tmpCnt{0};
        for(auto it : kv.key()) {
            ++tmpCnt;
            if(tmpCnt == 1) continue;
            //to do skip the first element
            decodeKeys.emplace_back(std::move(new std::string(it)));
        }
        watch::Watcher::EncodeKey(&dbKey, meta_.GetTableID(), decodeKeys);
    } else {
        dbKey = hashKey;
    }

    FLOG_DEBUG("WatchNotify haskkey:%s  key:%s", EncodeToHexString(hashKey).c_str(), EncodeToHexString(dbKey).c_str());

    //decltype(dbKey) dbPreKey{""};
    auto dbValue = kv.value();
    int64_t currDbVersion{version};
    auto watch_server = context_->range_server->watch_server_;

    int32_t evtCnt{1};
    std::vector<watchpb::Event> events;
    //get single key watcher list
    watch_server->GetKeyWatchers(evtType, vecNotifyWatcher, hashKey, dbKey, currDbVersion);

    watchpb::Event singleEvent;
    singleEvent.set_type(evtType);
    *singleEvent.mutable_kv() = kv;
    events.emplace_back(singleEvent);

    //start to send user kv to client
    int32_t watchCnt = vecNotifyWatcher.size();
    FLOG_DEBUG("single key notify:%" PRId32 " key:%s", watchCnt, EncodeToHexString(dbKey).c_str());
    if (watchCnt > 0) {
        Range::SendNotify(vecNotifyWatcher, events);
    }

    events.clear();
    if(hasPrefix) {
        watch_server->GetPrefixWatchers(evtType, vecPrefixNotifyWatcher, hashKey, dbKey, currDbVersion);

        watchCnt = vecPrefixNotifyWatcher.size();
        FLOG_DEBUG("prefix key notify:%" PRId32 " key:%s", watchCnt, EncodeToHexString(dbKey).c_str());

        if(watchCnt > 0) {
            //随机取用户版本作为起始版本
            int64_t startVersion{vecPrefixNotifyWatcher[0]->getKeyVersion()};

            std::vector<watch::CEventBufferValue> vecUpdKeys;
            vecUpdKeys.clear();
            int32_t cnt = eventBuffer->loadFromBuffer(dbKey, startVersion, vecUpdKeys);
            if (cnt < 0) {
                //get all from db
                FLOG_INFO("overlimit version in memory,get from db now. notify:%"
                                  PRId32
                                  " key:%s version:%"
                                  PRId64,
                          watchCnt, EncodeToHexString(dbKey).c_str(), startVersion);
                //use iterator
                std::string dbKeyEnd{""};
                dbKeyEnd.assign(dbKey);
                if( 0 != WatchCode::NextComparableBytes(dbKey.data(), dbKey.length(), dbKeyEnd)) {
                    //to do set error message
                    FLOG_ERROR("NextComparableBytes error.");
                    return -1;
                }
                //RANGE_LOG_DEBUG("WatchNotify key scope %s---%s", EncodeToHexString(dbKey).c_str(), EncodeToHexString(dbKeyEnd).c_str());

                //need to encode and decode
                std::shared_ptr<storage::Iterator> iterator(store_->NewIterator(dbKey, dbKeyEnd));
                uint32_t count{0};
                auto err = std::make_shared<errorpb::Error>();
                int64_t minVersion{0};
                int64_t maxVersion{0};

                for (int i = 0; iterator->Valid() ; ++i) {

                    auto tmpDbKey = iterator.get()->key();
                    auto tmpDbValue = iterator.get()->value();

                    watchpb::WatchKeyValue kv;
                    if(Status::kOk != WatchCode::DecodeKv(funcpb::kFuncPureGet, meta_.Get(), &kv, tmpDbKey, tmpDbValue, err.get())) {
                        //break;
                        continue;
                    }
                    //to judge version after decoding value and spliting version from value
                    if (minVersion > kv.version()) {
                        minVersion = kv.version();
                    }
                    if(maxVersion < kv.version()) {
                        maxVersion = kv.version();
                    }
                    if( kv.version() > startVersion) {
                        watchpb::Event evt;

                        for (int16_t i = 0; i < kv.key().size(); i++) {
                            evt.mutable_kv()->add_key(kv.key(i));
                        }

                        *evt.mutable_kv()->mutable_value() = kv.value();
                        evt.mutable_kv()->set_version(kv.version());
                        evt.set_type(evtType);

                        events.emplace_back(evt);
                        count++;
                    }

                    iterator->Next();
                }
                FLOG_DEBUG("load from db,count:%" PRIu32, count);

            } else if (0 == cnt) {
                FLOG_ERROR("doudbt no changing. notify:%"
                                   PRId32
                                   " key:%s", watchCnt, EncodeToHexString(dbKey).c_str());
            } else {
                FLOG_DEBUG("notify:%"
                                   PRId32
                                   " loadFromBuffer key:%s  data:%"
                                   PRId32, watchCnt, EncodeToHexString(dbKey).c_str(), cnt);
                for (auto i = 0; i < cnt; i++) {
                    //to do need to decode kv
                    watchpb::Event evt;
                    auto vecKeys = vecUpdKeys[i].key();
                    assert(vecKeys.size() == 1);
                    //FLOG_DEBUG(">>>%d...value:%s create-time:%" PRId64 " %d", vecUpdKeys[i].type(), vecUpdKeys[i].value().c_str(), vecUpdKeys[i].createTime(), vecKeys.size());

                    if(vecKeys.size() == 0) continue;

                    std::vector<std::string *> userKeys;
                    std::string userValue{""};
                    int64_t decodeVersion{0};
                    std::string decodeExt{""};

                    std::string encodeKey{vecKeys[0]};
                    std::string encodeValue{vecUpdKeys[i].value()};

                    watch::Watcher::DecodeKey(userKeys, encodeKey);
                    watch::Watcher::DecodeValue(&decodeVersion, &userValue, &decodeExt, encodeValue);

                    for (int16_t i = 0; i < userKeys.size(); i++) {
                        evt.mutable_kv()->add_key(*userKeys[i]);
                    }

                    *evt.mutable_kv()->mutable_value() = userValue;
                    evt.mutable_kv()->set_version(vecUpdKeys[i].version());
                    evt.set_type(vecUpdKeys[i].type());

                    events.emplace_back(evt);
                }
            }
        }
    }

    watchCnt = vecPrefixNotifyWatcher.size();
    evtCnt = events.size();
    if (hasPrefix && watchCnt > 0 && evtCnt > 0) {
        evtCnt = events.size();
        Range::SendNotify(vecPrefixNotifyWatcher, events, true);
    }

    return ret;
}

int32_t Range::SendNotify(const std::vector<watch::WatcherPtr>& vecNotify, const std::vector<watchpb::Event> &vecEvent, bool prefix)
{
    auto err = std::make_shared<errorpb::Error>();
    uint32_t watchCnt = uint32_t(vecNotify.size());
    auto watch_server = context_->range_server->watch_server_;

    watchpb::DsWatchResponse ds_resp_;


    for (uint32_t i = 0; i < vecEvent.size(); i++) {

        auto &tmpKv = vecEvent[i].kv();
        auto resp = ds_resp_.mutable_resp();
        auto evt = resp->add_events();

        evt->set_type(vecEvent[i].type());
        *evt = vecEvent[i];
        resp->set_code(Status::kOk);

        FLOG_INFO("SendNotify key:%s value:%s", tmpKv.key(0).c_str(), tmpKv.value().c_str());

        auto decodeKv = new watchpb::WatchKeyValue(tmpKv);
        evt->set_allocated_kv(decodeKv);
    }

    int32_t idx{0};

    for (auto w: vecNotify) {
        auto ds_resp = new watchpb::DsWatchResponse(ds_resp_);

        auto resp = ds_resp->mutable_resp();
        auto w_id = w->GetWatcherId();
        resp->set_watchid(w_id);

        w->Send(ds_resp);

        //delete watch
        watch::WatchCode del_ret;
        if (!prefix && w->GetType() == watch::WATCH_KEY) {


            del_ret = watch_server->DelKeyWatcher(w);

            if (del_ret) {
                RANGE_LOG_WARN(" Watch-Notify DelKeyWatcher (%"
                                  PRId32
                                  "/%"
                                  PRId32
                                  ")>>>[watch_id][%"
                                  PRId64
                                  "]",
                             idx, uint32_t(watchCnt), w_id);
            } else {
                RANGE_LOG_WARN(" DelKeyWatcher success. watch_id:%"
                                   PRIu64, w_id);
            }
        }

        if (prefix && w->GetType() == watch::WATCH_KEY) {
            del_ret = watch_server->DelPrefixWatcher(w);

            if (del_ret) {
                RANGE_LOG_WARN(" Watch-Notify DelPrefixWatcher (%"
                                  PRId32
                                  "/%"
                                  PRIu32
                                  ")>>>[watch_id][%"
                                  PRId64
                                  "]",
                               idx, uint32_t(watchCnt), w_id);
            } else {
                RANGE_LOG_WARN(" DelPrefixWatcher success. watch_id:%"
                                   PRIu64, w_id);
            }
        }

     } //end notify

    return idx;
}


}  // namespace range
}  // namespace dataserver
}  // namespace sharkstore
