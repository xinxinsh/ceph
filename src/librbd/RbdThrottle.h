// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef CEPH_LIBRBD_RBDTHROTTLE_H
#define CEPH_LIBRBD_RBDTHROTTLE_H

#include "include/Context.h"
#include "common/Cond.h"

#define THROTTLE_VALUE_MAX 1000000000000000LL
#define NANOSECONDS_PER_SECOND 1000000000LL

namespace librbd {

class ImageCtx;
typedef enum {
    THROTTLE_TPS_TOTAL = 0,
    THROTTLE_TPS_READ,
    THROTTLE_TPS_WRITE,
    THROTTLE_OPS_TOTAL,
    THROTTLE_OPS_READ,
    THROTTLE_OPS_WRITE,
    BUCKETS_COUNT,
} BucketType;


struct LeakyBucket {
    double  avg;              /* average goal in units per second */
    double  max;              /* leaky bucket max burst in units */
    double  level;            /* bucket level in units */ 
};

/* The following structure is used to configure a ThrottleState
 * It contains a bit of state: the bucket field of the LeakyBucket structure.
 * However it allows to keep the code clean and the bucket field is reset to
 * zero at the right time.
 */
struct ThrottleConfig {
  LeakyBucket buckets[BUCKETS_COUNT]; /* leaky buckets */
  CephContext *cct;
  ThrottleConfig(CephContext *_cct) : cct(_cct) {}
  uint64_t throttle_do_compute_wait(double limit, double extra);
  uint64_t throttle_compute_wait(LeakyBucket *bkt);
  void throttle_leak_bucket(LeakyBucket *bkt, uint64_t delta_ns);
  void throttle_config();
  bool throttle_is_valid(); 
};

class ThrottleState {
public:
  ThrottleState(ImageCtx *image_ctx); 
  ImageCtx &m_image_ctx;
  ThrottleConfig cfg;       /* configuration */
  void throttle_schedule_timer(bool is_write, size_t);
  void throttle_account(bool is_write, uint64_t size);
  uint64_t throttle_compute_wait_for(bool is_write);
  bool throttle_compute_timer(bool is_write, utime_t now, utime_t *next_timestamp);
  void throttle_do_leak(utime_t now);

private:
  utime_t previous_leak;    /* timestamp of the last leak done */
};

}

#endif

