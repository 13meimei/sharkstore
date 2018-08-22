#include "range_test_fixture.h"

#include "storage/meta_store.h"
#include "base/util.h"

#include "helper_util.h"
#include "helper/table.h"
#include "helper/mock/socket_session_mock.h"

namespace sharkstore {
namespace test {
namespace helper {

using namespace sharkstore::dataserver;
using namespace sharkstore::dataserver::common;
using namespace google::protobuf;

void RangeTestFixture::SetUp() {
    table_ = CreateAccountTable();

    context_.reset(new mock::RangeContextMock);
    auto s = context_->Init();
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto meta = MakeRangeMeta(table_.get());
    range_.reset(new range::Range(context_.get(), meta));
    s = range_->Initialize();
    ASSERT_TRUE(s.ok()) << s.ToString();
}

void RangeTestFixture::TearDown() {
    if (context_) {
        context_->Destroy();
    }
}

ProtoMessage* NewMsg(const Message& req) {
    auto msg = new common::ProtoMessage;
    msg->begin_time = getticks();
    msg->expire_time = getticks() + 1000;
    msg->session_id = randomInt();
    msg->header.msg_id = randomInt();
    msg->msg_id = msg->header.msg_id;

    auto len = req.ByteSizeLong();
    msg->body.resize(len);
    req.SerializeToArray(msg->body.data(), len);

    return msg;
}

Status RangeTestFixture::getResult(google::protobuf::Message *resp) {
    auto session_mock = dynamic_cast<SocketSessionMock*>(context_->SocketSession());
    if (!session_mock->GetResult(resp)) {
        return Status(Status::kUnexpected, "could not get result", "");
    } else {
        return Status::OK();
    }
}

Status RangeTestFixture::TestInsert(DsInsertRequest &req, DsInsertResponse *resp) {
    auto msg = NewMsg(req);
    range_->Insert(msg, req);
    return getResult(resp);
}

Status RangeTestFixture::TestSelect(DsSelectRequest& req, DsSelectResponse* resp) {
    auto msg = NewMsg(req);
    range_->Select(msg, req);
    return getResult(resp);
}

Status RangeTestFixture::TestDelete(DsDeleteRequest& req, DsDeleteResponse* resp) {
    auto msg = NewMsg(req);
    range_->Delete(msg, req);
    return getResult(resp);
}


} /* namespace helper */
} /* namespace test */
} /* namespace sharkstore */
