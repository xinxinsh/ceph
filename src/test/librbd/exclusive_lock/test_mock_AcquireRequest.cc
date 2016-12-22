// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librbd/mock/MockImageCtx.h"
#include "test/librbd/mock/MockImageState.h"
#include "test/librbd/mock/MockJournal.h"
#include "test/librbd/mock/MockJournalPolicy.h"
#include "test/librbd/mock/MockObjectMap.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/exclusive_lock/AcquireRequest.h"
#include "librbd/exclusive_lock/GetLockerRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>

namespace librbd {
namespace {

struct MockTestImageCtx : public librbd::MockImageCtx {
  MockTestImageCtx(librbd::ImageCtx &image_ctx)
    : librbd::MockImageCtx(image_ctx) {
  }
};

} // anonymous namespace

namespace exclusive_lock {

template <>
struct GetLockerRequest<librbd::MockTestImageCtx> {
  Locker *locker;
  Context *on_finish;

  static GetLockerRequest *s_instance;
  static GetLockerRequest *create(librbd::MockTestImageCtx &image_ctx,
                                  Locker *locker, Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->locker = locker;
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  GetLockerRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

GetLockerRequest<librbd::MockTestImageCtx> *GetLockerRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace exclusive_lock

namespace image {

template<>
struct RefreshRequest<librbd::MockTestImageCtx> {
  static RefreshRequest *s_instance;
  Context *on_finish;

  static RefreshRequest *create(librbd::MockTestImageCtx &image_ctx,
                                bool acquire_lock_refresh,
                                Context *on_finish) {
    EXPECT_TRUE(acquire_lock_refresh);
    assert(s_instance != nullptr);
    s_instance->on_finish = on_finish;
    return s_instance;
  }

  RefreshRequest() {
    s_instance = this;
  }
  MOCK_METHOD0(send, void());
};

RefreshRequest<librbd::MockTestImageCtx> *RefreshRequest<librbd::MockTestImageCtx>::s_instance = nullptr;

} // namespace image
} // namespace librbd

// template definitions
#include "librbd/Journal.cc"
#include "librbd/exclusive_lock/AcquireRequest.cc"
template class librbd::exclusive_lock::AcquireRequest<librbd::MockImageCtx>;

namespace librbd {
namespace exclusive_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

static const std::string TEST_COOKIE("auto 123");

class TestMockExclusiveLockAcquireRequest : public TestMockFixture {
public:
  typedef AcquireRequest<MockTestImageCtx> MockAcquireRequest;
  typedef GetLockerRequest<MockTestImageCtx> MockGetLockerRequest;
  typedef ExclusiveLock<MockTestImageCtx> MockExclusiveLock;
  typedef librbd::image::RefreshRequest<MockTestImageCtx> MockRefreshRequest;

  void expect_test_features(MockImageCtx &mock_image_ctx, uint64_t features,
                            bool enabled) {
    EXPECT_CALL(mock_image_ctx, test_features(features))
                  .WillOnce(Return(enabled));
  }

  void expect_lock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("lock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_unlock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("unlock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_is_refresh_required(MockImageCtx &mock_image_ctx, bool required) {
    EXPECT_CALL(*mock_image_ctx.state, is_refresh_required())
      .WillOnce(Return(required));
  }

  void expect_refresh(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(*mock_image_ctx.state, acquire_lock_refresh(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_create_object_map(MockImageCtx &mock_image_ctx,
                                MockObjectMap *mock_object_map) {
    EXPECT_CALL(mock_image_ctx, create_object_map(_))
                  .WillOnce(Return(mock_object_map));
  }

  void expect_open_object_map(MockImageCtx &mock_image_ctx,
                              MockObjectMap &mock_object_map, int r) {
    EXPECT_CALL(mock_object_map, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_object_map(MockImageCtx &mock_image_ctx,
                              MockObjectMap &mock_object_map) {
    EXPECT_CALL(mock_object_map, close(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_create_journal(MockImageCtx &mock_image_ctx,
                             MockJournal *mock_journal) {
    EXPECT_CALL(mock_image_ctx, create_journal())
                  .WillOnce(Return(mock_journal));
  }

  void expect_open_journal(MockImageCtx &mock_image_ctx,
                           MockJournal &mock_journal, int r) {
    EXPECT_CALL(mock_journal, open(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_close_journal(MockImageCtx &mock_image_ctx,
                            MockJournal &mock_journal) {
    EXPECT_CALL(mock_journal, close(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_get_journal_policy(MockImageCtx &mock_image_ctx,
                                 MockJournalPolicy &mock_journal_policy) {
    EXPECT_CALL(mock_image_ctx, get_journal_policy())
                  .WillOnce(Return(&mock_journal_policy));
  }

  void expect_allocate_journal_tag(MockImageCtx &mock_image_ctx,
                                   MockJournalPolicy &mock_journal_policy,
                                   int r) {
    EXPECT_CALL(mock_journal_policy, allocate_tag_on_lock(_))
                  .WillOnce(CompleteContext(r, mock_image_ctx.image_ctx->op_work_queue));
  }

  void expect_get_locker(MockTestImageCtx &mock_image_ctx,
                         MockGetLockerRequest &mock_get_locker_request,
                         const Locker &locker, int r) {
    EXPECT_CALL(mock_get_locker_request, send())
      .WillOnce(Invoke([&mock_image_ctx, &mock_get_locker_request, locker, r]() {
          *mock_get_locker_request.locker = locker;
          mock_image_ctx.image_ctx->op_work_queue->queue(
            mock_get_locker_request.on_finish, r);
        }));
  }

  void expect_list_watchers(MockImageCtx &mock_image_ctx, int r,
                            const std::string &address, uint64_t watch_handle) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               list_watchers(mock_image_ctx.header_oid, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      obj_watch_t watcher;
      strcpy(watcher.addr, (address + ":0/0").c_str());
      watcher.cookie = watch_handle;

      std::list<obj_watch_t> watchers;
      watchers.push_back(watcher);

      expect.WillOnce(DoAll(SetArgPointee<1>(watchers), Return(0)));
    }
  }

  void expect_blacklist_add(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_rados_client(), blacklist_add(_, _))
                  .WillOnce(Return(r));
  }

  void expect_break_lock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("break_lock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_flush_notifies(MockImageCtx &mock_image_ctx) {
    EXPECT_CALL(*mock_image_ctx.image_watcher, flush(_))
                  .WillOnce(CompleteContext(0, mock_image_ctx.image_ctx->op_work_queue));
  }
};

TEST_F(TestMockExclusiveLockAcquireRequest, Success) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap mock_object_map;
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, &mock_object_map);
  expect_open_object_map(mock_image_ctx, mock_object_map, 0);

  MockJournal mock_journal;
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
  expect_create_journal(mock_image_ctx, &mock_journal);
  expect_open_journal(mock_image_ctx, mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, SuccessRefresh) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh(mock_image_ctx, 0);

  MockObjectMap mock_object_map;
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, false);
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, false);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, SuccessJournalDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap mock_object_map;
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, &mock_object_map);
  expect_open_object_map(mock_image_ctx, mock_object_map, 0);

  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, false);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, SuccessObjectMapDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, false);

  MockJournal mock_journal;
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
  expect_create_journal(mock_image_ctx, &mock_journal);
  expect_open_journal(mock_image_ctx, mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, RefreshError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, true);
  expect_refresh(mock_image_ctx, -EINVAL);
  expect_unlock(mock_image_ctx, 0);

  C_SaferCond *acquire_ctx = new C_SaferCond();
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, JournalError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap *mock_object_map = new MockObjectMap();
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, mock_object_map);
  expect_open_object_map(mock_image_ctx, *mock_object_map, 0);

  MockJournal *mock_journal = new MockJournal();
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
  expect_create_journal(mock_image_ctx, mock_journal);
  expect_open_journal(mock_image_ctx, *mock_journal, -EINVAL);
  expect_close_journal(mock_image_ctx, *mock_journal);
  expect_close_object_map(mock_image_ctx, *mock_object_map);
  expect_unlock(mock_image_ctx, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, AllocateJournalTagError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap *mock_object_map = new MockObjectMap();
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, mock_object_map);
  expect_open_object_map(mock_image_ctx, *mock_object_map, 0);

  MockJournal *mock_journal = new MockJournal();
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
  expect_create_journal(mock_image_ctx, mock_journal);
  expect_open_journal(mock_image_ctx, *mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, -EPERM);
  expect_close_journal(mock_image_ctx, *mock_journal);
  expect_close_object_map(mock_image_ctx, *mock_object_map);
  expect_unlock(mock_image_ctx, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(-EPERM, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, LockBusy) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, 0);
  expect_lock(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, GetLockInfoError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, -EINVAL);
  expect_handle_prepare_lock_complete(mock_image_ctx);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, GetLockInfoEmpty) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request, {}, -ENOENT);
  expect_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, GetWatchersError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_list_watchers(mock_image_ctx, -EINVAL, "dead client", 123);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, GetWatchersAlive) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_list_watchers(mock_image_ctx, 0, "1.2.3.4", 123);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EAGAIN, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, BlacklistDisabled) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);
  mock_image_ctx.blacklist_on_break_lock = false;

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_break_lock(mock_image_ctx, 0);
  expect_lock(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, BlacklistError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, BreakLockMissing) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -ENOENT);
  expect_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, BreakLockError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockTestImageCtx mock_image_ctx(*ictx);
  MockGetLockerRequest mock_get_locker_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_locker(mock_image_ctx, mock_get_locker_request,
                    {entity_name_t::CLIENT(1), "auto 123", "1.2.3.4:0/0", 123},
                    0);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       nullptr, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockExclusiveLockAcquireRequest, OpenObjectMapError) {
  REQUIRE_FEATURE(RBD_FEATURE_EXCLUSIVE_LOCK);

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_flush_notifies(mock_image_ctx);
  expect_lock(mock_image_ctx, 0);
  expect_is_refresh_required(mock_image_ctx, false);

  MockObjectMap *mock_object_map = new MockObjectMap();
  expect_test_features(mock_image_ctx, RBD_FEATURE_OBJECT_MAP, true);
  expect_create_object_map(mock_image_ctx, mock_object_map);
  expect_open_object_map(mock_image_ctx, *mock_object_map, -EFBIG);

  MockJournal mock_journal;
  MockJournalPolicy mock_journal_policy;
  expect_test_features(mock_image_ctx, RBD_FEATURE_JOURNALING, true);
  expect_create_journal(mock_image_ctx, &mock_journal);
  expect_open_journal(mock_image_ctx, mock_journal, 0);
  expect_get_journal_policy(mock_image_ctx, mock_journal_policy);
  expect_allocate_journal_tag(mock_image_ctx, mock_journal_policy, 0);

  C_SaferCond acquire_ctx;
  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx,
                                                       TEST_COOKIE,
                                                       &acquire_ctx, &ctx);
  req->send();
  ASSERT_EQ(0, acquire_ctx.wait());
  ASSERT_EQ(0, ctx.wait());
  ASSERT_EQ(nullptr, mock_image_ctx.object_map);
}

} // namespace exclusive_lock
} // namespace librbd
