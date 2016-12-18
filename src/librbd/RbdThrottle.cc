#include "librbd/RbdThrottle.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::RbdThrottle: "

namespace librbd {

ThrottleState::ThrottleState(ImageCtx *image_ctx)
: m_image_ctx(*image_ctx),cfg(image_ctx->cct)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " " << ": ictx=" << image_ctx << dendl;

  previous_leak = ceph_clock_now(cct);
}  

/* compute the timer for this type of operation
 *
 * @is_write:   the type of operation
 * @now:        the current clock timestamp
 * @next_timestamp: the resulting timer
 * @ret:        true if a timer must be set
 */
bool ThrottleState::throttle_compute_timer(bool is_write,
                                   utime_t now,
                                   utime_t *next_timestamp)
{
  uint64_t wait;
  CephContext *cct = m_image_ctx.cct;

  /* leak proportionally to the time elapsed */
  throttle_do_leak(now);
  ldout(cct, 20) << __func__ << " now: " << now << dendl;
  /* compute the wait time if any */
  wait = throttle_compute_wait_for(is_write);
  ldout(cct, 20) << __func__ << " wait: " << wait << dendl;
 
  /* if the code must wait compute when the next timer should fire */
  if (wait) {
    uint64_t tmp_sec = wait/NANOSECONDS_PER_SECOND;
    uint64_t tmp_nsec =  wait%NANOSECONDS_PER_SECOND;
    if(tmp_sec) {
      now.tv.tv_sec += tmp_sec;	
    }
    now.tv.tv_nsec += tmp_nsec;
    *next_timestamp += now;
    ldout(cct, 20) << __func__ << " next_timestamp: " << *next_timestamp << dendl;
    return true;
  }

    /* else no need to wait at all */
  *next_timestamp = now;
  return false;
}

/* Calculate the time delta since last leak and make proportionals leaks
 *
 * @now:      the current timestamp 
 */
void ThrottleState::throttle_do_leak(utime_t now)
{
  CephContext *cct = m_image_ctx.cct;
  /* compute the time elapsed since the last leak */
  uint64_t delta_ns = now.to_nsec() - previous_leak.to_nsec();
  int i;
  ldout(cct, 20) << __func__ << " delta_ns: " << delta_ns << dendl;
  previous_leak = now;

  if (delta_ns <= 0) {
    return;
  }

  /* make each bucket leak */
  for (i = 0; i < BUCKETS_COUNT; i++) {
    cfg.throttle_leak_bucket(&cfg.buckets[i], delta_ns);    
  }
}

/* This function compute the time that must be waited while this IO
 *
 * @is_write:   true if the current IO is a write, false if it's a read
 * @ret:        time to wait
 */
uint64_t ThrottleState::throttle_compute_wait_for(bool is_write)
{
  BucketType to_check[2][4] = { {THROTTLE_TPS_TOTAL,
                                   THROTTLE_OPS_TOTAL,
                                   THROTTLE_TPS_READ,
                                   THROTTLE_OPS_READ},
                                  {THROTTLE_TPS_TOTAL,
                                   THROTTLE_OPS_TOTAL,
                                   THROTTLE_TPS_WRITE,
                                   THROTTLE_OPS_WRITE}, };
  uint64_t wait, max_wait = 0;
  int i;
  CephContext *cct = m_image_ctx.cct;

  for (i = 0; i < 4; i++) {
    BucketType index = to_check[is_write][i];
    wait = cfg.throttle_compute_wait(&cfg.buckets[index]);
    ldout(cct, 20) << __func__ << " wait: " << wait << dendl;
    if (wait > max_wait) {
    	max_wait = wait;
    }
  }

  return max_wait;
}

/* Schedule the read or write timer if needed
 *
 * @bytes:     the number of bytes for this I/O
 * @is_write: the type of operation (read/write)
 */

void ThrottleState::throttle_schedule_timer( bool is_write, size_t len)
{
  utime_t now;
  CephContext *cct = m_image_ctx.cct;
  now = ceph_clock_now(m_image_ctx.cct);
  utime_t next_timestamp;
  bool must_wait;
    
  must_wait = throttle_compute_timer(is_write,
                                     now,
                                     &next_timestamp);
	
  ldout(cct, 20) << __func__ << " must-wait:" << must_wait << " next_timestamp:" <<next_timestamp << dendl;
  /* request not throttled */
  if (must_wait) {      
    Mutex::Locker l(m_image_ctx.throttle_lock);
    Cond cond;
    cond.WaitUntil(m_image_ctx.throttle_lock, next_timestamp);
  }

  throttle_account(is_write, len); 
}

/* do the accounting for this operation
 *
 * @is_write: the type of operation (read/write)
 * @size:     the size of the operation
 */
void ThrottleState::throttle_account(bool is_write, size_t size)
{
  const BucketType bucket_types_size[2][2] = {
      { THROTTLE_TPS_TOTAL, THROTTLE_TPS_READ },
      { THROTTLE_TPS_TOTAL, THROTTLE_TPS_WRITE }
  };
  const BucketType bucket_types_units[2][2] = {
      { THROTTLE_OPS_TOTAL, THROTTLE_OPS_READ },
      { THROTTLE_OPS_TOTAL, THROTTLE_OPS_WRITE }
  };
  double units = 1.0;
  unsigned i;
	
  for (i = 0; i < 2; i++) {
    LeakyBucket *bkt;

    bkt = &cfg.buckets[bucket_types_size[is_write][i]];
    bkt->level += size;
    bkt = &cfg.buckets[bucket_types_units[is_write][i]];
    bkt->level += units;
  }

}

/* do the real job of computing the time to wait
 *
 * @limit: the throttling limit
 * @extra: the number of operation to delay
 * @ret:   the time to wait in ns
 */
uint64_t ThrottleConfig::throttle_do_compute_wait(double limit, double extra)
{
  double wait = extra * NANOSECONDS_PER_SECOND;
  wait /= limit;
  return wait;
}

/* This function compute the wait time in ns that a leaky bucket should trigger
 *
 * @bkt: the leaky bucket we operate on
 * @ret: the resulting wait time in ns or 0 if the operation can go through
 */
uint64_t ThrottleConfig::throttle_compute_wait(LeakyBucket *bkt)
{
  double extra; /* the number of extra units blocking the io */

  if (!bkt->avg) {
    return 0;
  }

  /* If the bucket is full then we have to wait */
  extra = bkt->level - bkt->max;
    	
  ldout(cct, 20) << __func__ << " avg: " << bkt->avg << " extra " << extra << dendl;
  if (extra > 0) {
    return throttle_do_compute_wait(bkt->avg, extra);
  }

   /* If the bucket is not full yet we have to make sure that we
   * fulfill the goal of bkt->max units per second. */
    
  return 0;
}

/* This function make a bucket leak
 *
 * @bkt:   the bucket to make leak
 * @delta_ns: the time delta
 */
void ThrottleConfig::throttle_leak_bucket(LeakyBucket *bkt, uint64_t delta_ns)
{
  double leak;

  /* compute how much to leak */
  leak = (bkt->avg * (double) delta_ns) / NANOSECONDS_PER_SECOND;

  /* make the bucket leak */
  bkt->level = MAX(bkt->level - leak, 0);
  ldout(cct, 20) << __func__ << " level: " << bkt->level << dendl;
}

/* check if a throttling configuration is valid
 * @ret: true if valid else false
 */
bool ThrottleConfig::throttle_is_valid()
{
  int i;
  bool bps_flag, ops_flag;
	
  bps_flag = buckets[THROTTLE_TPS_TOTAL].avg &&
	     (buckets[THROTTLE_TPS_READ].avg ||
	      buckets[THROTTLE_TPS_WRITE].avg);
	
  ops_flag = buckets[THROTTLE_OPS_TOTAL].avg &&
	     (buckets[THROTTLE_OPS_READ].avg ||
	      buckets[THROTTLE_OPS_WRITE].avg);
	
  if (bps_flag || ops_flag) {
    return false;
  }
	
  for (i = 0; i < BUCKETS_COUNT; i++) {
    if (buckets[i].avg < 0 ||
	buckets[i].avg > THROTTLE_VALUE_MAX) {
	  return false;
    }	 
  }

  return true;
}

/* Used to configure the throttle */
void ThrottleConfig::throttle_config()
{
  int i;

  buckets[THROTTLE_TPS_TOTAL].avg = cct->_conf->rbd_throttle_tps_total;
  buckets[THROTTLE_TPS_READ].avg = cct->_conf->rbd_throttle_tps_read;
  buckets[THROTTLE_TPS_WRITE].avg = cct->_conf->rbd_throttle_tps_write;
  buckets[THROTTLE_OPS_TOTAL].avg = cct->_conf->rbd_throttle_ops_total;
  buckets[THROTTLE_OPS_READ].avg = cct->_conf->rbd_throttle_ops_read;
  buckets[THROTTLE_OPS_WRITE].avg = cct->_conf->rbd_throttle_ops_write;	
    
  for (i = 0; i < BUCKETS_COUNT; i++) {
    buckets[i].level = 0;
  }

  for (i = 0; i < BUCKETS_COUNT; i++) {
    buckets[i].max = buckets[i].level/10;
  }
	
}

}

