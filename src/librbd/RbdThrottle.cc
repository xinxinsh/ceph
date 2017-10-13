#include "librbd/RbdThrottle.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::RbdThrottle: " << __func__ << ":" << __LINE__ << " "

namespace librbd {

ThrottleState::ThrottleState(ImageCtx *image_ctx)
: m_image_ctx(*image_ctx),cfg(image_ctx->cct)
{
  cct = m_image_ctx.cct;
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

  /* leak proportionally to the time elapsed */
  ldout(cct, 20) << " now: " << now << dendl;
  throttle_do_leak(now);
  
  /* compute the wait time if any */
  wait = throttle_compute_wait_for(is_write);
  ldout(cct, 20) << " wait: " << wait << dendl;
 
  /* if the code must wait compute when the next timer should fire */
  if (wait) {
    uint64_t tmp_sec = wait / NANOSECONDS_PER_SECOND;
    uint64_t tmp_nsec = wait % NANOSECONDS_PER_SECOND;

	now.tv.tv_nsec += tmp_nsec;
	if (now.tv.tv_nsec > NANOSECONDS_PER_SECOND) {
		now.tv.tv_nsec -= NANOSECONDS_PER_SECOND;
		tmp_sec += 1;
	}
		
	if(tmp_sec) {
      now.tv.tv_sec += tmp_sec;	
    }
	
    *next_timestamp += now;
    ldout(cct, 20) << " next_timestamp: " << *next_timestamp << dendl;
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
  /* compute the time elapsed since the last leak */
  uint64_t delta_ns = now.to_nsec() - previous_leak.to_nsec();
  ldout(cct, 20) << " delta_ns: " << delta_ns << dendl;
  
  previous_leak = now;

  if (delta_ns <= 0) {
    return;
  }

  /* make each bucket leak */
  for (int i = 0; i < BUCKETS_COUNT; i++) {
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

  for (int i = 0; i < 4; i++) {
    BucketType index = to_check[is_write][i];
    wait = cfg.throttle_compute_wait(&cfg.buckets[index]);
    if (wait > max_wait) {
    	max_wait = wait;
    }
  }

  ldout(cct, 20) << " wait: " << wait << dendl;
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
  now = ceph_clock_now(m_image_ctx.cct);
  utime_t next_timestamp;
  bool must_wait;
    
  must_wait = throttle_compute_timer(is_write,
                                     now,
                                     &next_timestamp);
	
  ldout(cct, 20) << " must-wait:" << must_wait << " next_timestamp:" <<next_timestamp << dendl;
  /* request not throttled */
  if (must_wait) { 
  	Cond cond;
    Mutex::Locker l(m_image_ctx.throttle_lock);
    
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

  /* if cfg.op_size is defined and smaller than size we compute unit count */
  if (cfg.op_size && size > cfg.op_size) {
        units = (double) size / cfg.op_size;
  }
	
  for (unsigned i = 0; i < 2; i++) {
    LeakyBucket *bkt;

    bkt = &cfg.buckets[bucket_types_size[is_write][i]];
    bkt->level += size;
	if (bkt->burst_length > 1)
		bkt->burst_level += size;
	
    bkt = &cfg.buckets[bucket_types_units[is_write][i]];
    bkt->level += units;
	if (bkt->burst_length > 1)
		bkt->burst_level += units;
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
  extra = bkt->level - bkt->max * bkt->burst_length;
    	
  ldout(cct, 20) << " avg: " << bkt->avg << " extra: " << extra << dendl;
  if (extra > 0) {
    return throttle_do_compute_wait(bkt->avg, extra);
  }

  /* If the bucket is not full yet we have to make sure that we
   * fulfill the goal of bkt->max units per second. */
  if (bkt->burst_length > 1) {
    /* We use 1/10 of the max value to smooth the throttling.
     * See throttle_fix_bucket() for more details. */
    extra = bkt->burst_level - bkt->max / 10;
    if (extra > 0) {
        return throttle_do_compute_wait(bkt->max, extra);
    }
  }
    
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
  ldout(cct, 20) << " level: " << bkt->level << dendl;

  /* if we allow bursts for more than one second we also need to
   * keep track of bkt->burst_level so the bkt->max goal per second
   * is attained */
  if (bkt->burst_length > 1) {
    leak = (bkt->max * (double) delta_ns) / NANOSECONDS_PER_SECOND;
    bkt->burst_level = MAX(bkt->burst_level - leak, 0);
  }
}

/* check if a throttling configuration is valid
 * @ret: true if valid else false
 */
bool ThrottleConfig::throttle_is_valid()
{
  int i;
  bool tps_flag, ops_flag;
  bool tps_max_flag, ops_max_flag;
	
  tps_flag = buckets[THROTTLE_TPS_TOTAL].avg &&
	     (buckets[THROTTLE_TPS_READ].avg ||
	      buckets[THROTTLE_TPS_WRITE].avg);
	
  ops_flag = buckets[THROTTLE_OPS_TOTAL].avg &&
	     (buckets[THROTTLE_OPS_READ].avg ||
	      buckets[THROTTLE_OPS_WRITE].avg);

  tps_max_flag = buckets[THROTTLE_TPS_TOTAL].max &&
  			(buckets[THROTTLE_TPS_READ].max ||
  			buckets[THROTTLE_TPS_WRITE].max);

  ops_max_flag = buckets[THROTTLE_OPS_TOTAL].max &&
  			(buckets[THROTTLE_OPS_READ].max ||
  			buckets[THROTTLE_OPS_WRITE].max);
	
  if (tps_flag || ops_flag || tps_max_flag || ops_max_flag) {
  	lderr(cct) << "bps/iops/max total values and read/write values"
                   " cannot be used at the same time" << dendl;
    return false;
  }

  if (op_size &&
    !buckets[THROTTLE_OPS_TOTAL].avg &&
    !buckets[THROTTLE_OPS_READ].avg &&
    !buckets[THROTTLE_OPS_WRITE].avg) {
    lderr(cct) << "iops size requires an iops value to be set" << dendl;
    return false;
  }
	
  for (i = 0; i < BUCKETS_COUNT; i++) {
    if (buckets[i].avg < 0 ||
        buckets[i].max < 0 ||
        buckets[i].avg > THROTTLE_VALUE_MAX ||
        buckets[i].max > THROTTLE_VALUE_MAX) {
        lderr(cct) << "bps/iops/max values must be within [0, "<< THROTTLE_VALUE_MAX <<"]" << dendl;
        return false;
    }

    if (!buckets[i].burst_length) {
        lderr(cct) << "the burst length cannot be 0" << dendl;
        return false;
    }

    if (buckets[i].burst_length > 1 && !buckets[i].max) {
        lderr(cct) << "burst length set without burst rate" << dendl;
        return false;
    }

    if (buckets[i].max && !buckets[i].avg) {
        lderr(cct) << "bps_max/iops_max require corresponding"
                   " bps/iops values" << dendl;
        return false;
    }

    if (buckets[i].max && buckets[i].max < buckets[i].avg) {
        lderr(cct) << "bps_max/iops_max cannot be lower than bps/iops" << dendl;
        return false;
    }
    if(buckets[i].max == 0) {
       buckets[i].max = buckets[i].avg / 10;
    }
  }

  return true;
}

/* convert map a throttling configuration
 *      @pairs a throttling map
 *      @key a throttling configuration key
 *      @val: value of a throttling configuration key
 *            */

double ThrottleConfig::map_to_cfg(map<std::string, double> *pairs, const string &key, const double val)
{
   map<std::string, double>::iterator it = pairs->find(key);
   if(it == pairs->end())
     return val;
   else
     return it->second;
}

/* convert throttling configuration to map
 *     @pairs a throttling map
 *     @key a throttling configuration key
 *     @val: value of a throttling configuration key
 *           */
void ThrottleConfig::cfg_to_map(map<std::string, bufferlist> *pairs, const string key, const double val)
{
      const string metadata_throttle_prefix = "rbd_throttle_";
      int prec = std::numeric_limits<int>::digits10;
      ostringstream throttle_value;
      throttle_value.precision(prec);
      throttle_value << val;
      (*pairs)[(metadata_throttle_prefix+key)].append(throttle_value.str());
}

/* Used to configure the throttle */
void ThrottleConfig::throttle_config(uint64_t image_size)
{
  for (int i = 0; i < BUCKETS_COUNT; ++i) {
  	memset((char*)&buckets[i], 0,sizeof(LeakyBucket));
  }

  //overwrite max value based on throttle_mode
  uint64_t image_gsize = image_size >> 30;
  switch (cct->_conf->rbd_throttle_mode) {
  	case THROTTLE_MODE_HDD:
		ldout(cct, 20) << "throttle mode HDD:(ops = 400 and tps = 40MB/s) " << dendl;

                buckets[THROTTLE_TPS_READ].avg = 40 << 20;
                buckets[THROTTLE_OPS_READ].avg = 400;
                buckets[THROTTLE_TPS_READ].max = 40 << 20;
                buckets[THROTTLE_OPS_READ].max = 400;
                buckets[THROTTLE_TPS_WRITE].avg = 40 << 20;
                buckets[THROTTLE_OPS_WRITE].avg = 400;
                buckets[THROTTLE_TPS_WRITE].max = 40 << 20;
                buckets[THROTTLE_OPS_WRITE].max = 400;

		break;
	case THROTTLE_MODE_EDD:
		ldout(cct, 20) << "throttle mode EDD:(ops = min(1000 + 6 * image_size, 3000) and "
			" tps = min(50 + size * 0.1, 80) MB/s) " << dendl;

                buckets[THROTTLE_TPS_READ].avg = (uint64_t)(MIN(50 + 0.1 *image_gsize, 80) *1024*1024);
                buckets[THROTTLE_OPS_READ].avg = MIN(1000 + 6 * image_gsize, 3000);
                buckets[THROTTLE_TPS_READ].max = 80 << 20;
                buckets[THROTTLE_OPS_READ].max = 3000;
                buckets[THROTTLE_TPS_WRITE].avg = (uint64_t)(MIN(50 + 0.1 *image_gsize, 80) *1024*1024);
                buckets[THROTTLE_OPS_WRITE].avg = MIN(1000 + 6 * image_gsize, 3000);
                buckets[THROTTLE_TPS_WRITE].max = 80 << 20;
                buckets[THROTTLE_OPS_WRITE].max = 3000;
		break;
	case THROTTLE_MODE_SSD:
		ldout(cct, 20) << "throttle mode SSD:(ops = min(30 * image_size, 20000) and "
			" tps = min(50 + size * 0.5, 256)) MB/s" << dendl;

                buckets[THROTTLE_TPS_READ].avg = (uint64_t)(MIN(50 + 0.5 *image_gsize, 256) *1024*1024);
                buckets[THROTTLE_OPS_READ].avg = MIN(30 * image_gsize, 20000);
                buckets[THROTTLE_TPS_READ].max = 256 << 20;
                buckets[THROTTLE_OPS_READ].max = 20000;
                buckets[THROTTLE_TPS_WRITE].avg = (uint64_t)(MIN(50 + 0.5 *image_gsize, 256) *1024*1024);
                buckets[THROTTLE_OPS_WRITE].avg = MIN(30 * image_gsize, 20000);
                buckets[THROTTLE_TPS_WRITE].max = 256 << 20;
                buckets[THROTTLE_OPS_WRITE].max = 20000;

		break;
	default:
		buckets[THROTTLE_TPS_TOTAL].avg = cct->_conf->rbd_throttle_tps_total;
		buckets[THROTTLE_TPS_READ].avg = cct->_conf->rbd_throttle_tps_read;
        buckets[THROTTLE_TPS_WRITE].avg = cct->_conf->rbd_throttle_tps_write;
        buckets[THROTTLE_OPS_TOTAL].avg = cct->_conf->rbd_throttle_ops_total;
        buckets[THROTTLE_OPS_READ].avg = cct->_conf->rbd_throttle_ops_read;
        buckets[THROTTLE_OPS_WRITE].avg = cct->_conf->rbd_throttle_ops_write;	  
		
        buckets[THROTTLE_TPS_TOTAL].max = cct->_conf->rbd_throttle_tps_total_max;
        buckets[THROTTLE_TPS_READ].max = cct->_conf->rbd_throttle_tps_read_max;
        buckets[THROTTLE_TPS_WRITE].max = cct->_conf->rbd_throttle_tps_write_max;
        buckets[THROTTLE_OPS_TOTAL].max = cct->_conf->rbd_throttle_ops_total_max;
        buckets[THROTTLE_OPS_READ].max = cct->_conf->rbd_throttle_ops_read_max;
        buckets[THROTTLE_OPS_WRITE].max = cct->_conf->rbd_throttle_ops_write_max;

  }

  buckets[THROTTLE_TPS_TOTAL].burst_length = cct->_conf->rbd_throttle_tps_total_max_length;
  buckets[THROTTLE_TPS_READ].burst_length = cct->_conf->rbd_throttle_tps_read_max_length;
  buckets[THROTTLE_TPS_WRITE].burst_length = cct->_conf->rbd_throttle_tps_write_max_length;
  buckets[THROTTLE_OPS_TOTAL].burst_length = cct->_conf->rbd_throttle_ops_total_max_length;
  buckets[THROTTLE_OPS_READ].burst_length = cct->_conf->rbd_throttle_ops_read_max_length;
  buckets[THROTTLE_OPS_WRITE].burst_length = cct->_conf->rbd_throttle_ops_write_max_length;

  op_size = cct->_conf->rbd_throttle_op_size;
}

}

