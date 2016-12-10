// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OBJECTCACHER_H
#define CEPH_OBJECTCACHER_H

#include "include/types.h"
#include "include/lru.h"
#include "include/Context.h"
#include "include/xlist.h"

#include "common/errno.h"
#include "common/Cond.h"
#include "common/Finisher.h"
#include "common/Thread.h"

#include "Objecter.h"
#include "Striper.h"

class CephContext;
class WritebackHandler;
class PerfCounters;

enum {
  l_objectcacher_first = 25000,

  l_objectcacher_cache_ops_hit, // ops we satisfy completely from cache
  l_objectcacher_cache_ops_miss, // ops we don't satisfy completely from cache

  l_objectcacher_cache_bytes_hit, // bytes read directly from cache

  l_objectcacher_cache_bytes_miss, // bytes we couldn't read directly

				   // from cache

  l_objectcacher_data_read, // total bytes read out
  l_objectcacher_data_written, // bytes written to cache
  l_objectcacher_data_flushed, // bytes flushed to WritebackHandler
  l_objectcacher_overwritten_in_flush, // bytes overwritten while
				       // flushing is in progress

  l_objectcacher_write_ops_blocked, // total write ops we delayed due
				    // to dirty limits
  l_objectcacher_write_bytes_blocked, // total number of write bytes
				      // we delayed due to dirty
				      // limits
  l_objectcacher_write_time_blocked, // total time in seconds spent
				     // blocking a write due to dirty
				     // limits

  l_objectcacher_last,
};

class LCache;
class ObjectCacher {
  PerfCounters *perfcounter;
 public:
  CephContext *cct;
  class Object;
  friend class LCache;
  struct ObjectSet;
  class C_ReadFinish;

  typedef void (*flush_set_callback_t) (void *p, ObjectSet *oset);

  // read scatter/gather
  struct OSDRead {
    vector<ObjectExtent> extents;
    snapid_t snap;
    bufferlist *bl;
    int fadvise_flags;
    OSDRead(snapid_t s, bufferlist *b, int f)
      : snap(s), bl(b), fadvise_flags(f) {}
  };

  OSDRead *prepare_read(snapid_t snap, bufferlist *b, int f) {
    return new OSDRead(snap, b, f);
  }

  // write scatter/gather
  struct OSDWrite {
    vector<ObjectExtent> extents;
    SnapContext snapc;
    bufferlist bl;
    ceph::real_time mtime;
    int fadvise_flags;
    ceph_tid_t journal_tid;
    OSDWrite(const SnapContext& sc, const bufferlist& b, ceph::real_time mt,
	     int f, ceph_tid_t _journal_tid)
      : snapc(sc), bl(b), mtime(mt), fadvise_flags(f),
	journal_tid(_journal_tid) {}
  };

  OSDWrite *prepare_write(const SnapContext& sc, const bufferlist &b,
			  ceph::real_time mt, int f, ceph_tid_t journal_tid) {
    return new OSDWrite(sc, b, mt, f, journal_tid);
  }



  // ******* BufferHead *********
  class BufferHead : public LRUObject {
  public:
    // states
    static const int STATE_MISSING = 0;
    static const int STATE_CLEAN = 1;
    static const int STATE_ZERO = 2;   // NOTE: these are *clean* zeros
    static const int STATE_DIRTY = 3;
    static const int STATE_RX = 4;
    static const int STATE_TX = 5;
    static const int STATE_ERROR = 6; // a read error occurred

  private:
    // my fields
    int state;
    int ref;
    struct {
      loff_t start, length;   // bh extent in object
    } ex;
    bool dontneed; //indicate bh don't need by anyone
    bool nocache; //indicate bh don't need by this caller

  public:
    Object *ob;
    bufferlist  bl;
    ceph_tid_t last_write_tid;  // version of bh (if non-zero)
    ceph_tid_t last_read_tid;   // tid of last read op (if any)
    ceph::real_time last_write;
    SnapContext snapc;
    ceph_tid_t journal_tid;
    int error; // holds return value for failed reads

    map<loff_t, list<Context*> > waitfor_read;

    // cons
    explicit BufferHead(Object *o) :
      state(STATE_MISSING),
      ref(0),
      dontneed(false),
      nocache(false),
      ob(o),
      last_write_tid(0),
      last_read_tid(0),
      journal_tid(0),
      error(0) {
      ex.start = ex.length = 0;
    }

    // extent
    loff_t start() const { return ex.start; }
    void set_start(loff_t s) { ex.start = s; }
    loff_t length() const { return ex.length; }
    void set_length(loff_t l) { ex.length = l; }
    loff_t end() const { return ex.start + ex.length; }
    loff_t last() const { return end() - 1; }

    // states
    void set_state(int s) {
      if (s == STATE_RX || s == STATE_TX) get();
      if (state == STATE_RX || state == STATE_TX) put();
      state = s;
    }
    int get_state() const { return state; }

    inline ceph_tid_t get_journal_tid() const {
      return journal_tid;
    }
    inline void set_journal_tid(ceph_tid_t _journal_tid) {
      journal_tid = _journal_tid;
    }

    bool is_missing() { return state == STATE_MISSING; }
    bool is_dirty() { return state == STATE_DIRTY; }
    bool is_clean() { return state == STATE_CLEAN; }
    bool is_zero() { return state == STATE_ZERO; }
    bool is_tx() { return state == STATE_TX; }
    bool is_rx() { return state == STATE_RX; }
    bool is_error() { return state == STATE_ERROR; }

    // reference counting
    int get() {
      assert(ref >= 0);
      if (ref == 0) lru_pin();
      return ++ref;
    }
    int put() {
      assert(ref > 0);
      if (ref == 1) lru_unpin();
      --ref;
      return ref;
    }

    void set_dontneed(bool v) {
      dontneed = v;
    }
    bool get_dontneed() {
      return dontneed;
    }

    void set_nocache(bool v) {
      nocache = v;
    }
    bool get_nocache() {
      return nocache;
    }

    inline bool can_merge_journal(BufferHead *bh) const {
      return (get_journal_tid() == bh->get_journal_tid());
    }

    struct ptr_lt {
      bool operator()(const BufferHead* l, const BufferHead* r) const {
	const Object *lob = l->ob;
	const Object *rob = r->ob;
	const ObjectSet *loset = lob->oset;
	const ObjectSet *roset = rob->oset;
	if (loset != roset)
	  return loset < roset;
	if (lob != rob)
	  return lob < rob;
	if (l->start() != r->start())
	  return l->start() < r->start();
	return l < r;
      }
    };
  };

  // ******* Object *********
  class Object : public LRUObject {
  private:
    // ObjectCacher::Object fields
    int ref;
    ObjectCacher *oc;
    sobject_t oid;
    friend struct ObjectSet;

  public:
    uint64_t object_no;
    ObjectSet *oset;
    xlist<Object*>::item set_item;
    object_locator_t oloc;
    uint64_t truncate_size, truncate_seq;

	//for local cache only: ObjMeta offset and first BufMeta
	loff_t oft, data_l;	
	
    bool complete;
    bool exists;

  public:
    map<loff_t, BufferHead*>     data;

    ceph_tid_t last_write_tid;  // version of bh (if non-zero)
    ceph_tid_t last_commit_tid; // last update commited.

    int dirty_or_tx;

    map< ceph_tid_t, list<Context*> > waitfor_commit;
    xlist<C_ReadFinish*> reads;

  public:
    Object(const Object& other);
    const Object& operator=(const Object& other);

    Object(ObjectCacher *_oc, sobject_t o, uint64_t ono, ObjectSet *os,
	   object_locator_t& l, uint64_t ts, uint64_t tq) :
      ref(0),
      oc(_oc),
      oid(o), object_no(ono), oset(os), set_item(this), oloc(l),
      truncate_size(ts), truncate_seq(tq), oft(-1), data_l(-1),
      complete(false), exists(true),
      last_write_tid(0), last_commit_tid(0),
      dirty_or_tx(0) {
      // add to set
      os->objects.push_back(&set_item);
    }
    ~Object() {
      reads.clear();
      assert(ref == 0);
      assert(data.empty());
      assert(dirty_or_tx == 0);
      set_item.remove_myself();
    }

    sobject_t get_soid() { return oid; }
    object_t get_oid() { return oid.oid; }
    snapid_t get_snap() { return oid.snap; }
    ObjectSet *get_object_set() { return oset; }
    string get_namespace() { return oloc.nspace; }
    uint64_t get_object_number() const { return object_no; }

    object_locator_t& get_oloc() { return oloc; }
    void set_object_locator(object_locator_t& l) { oloc = l; }

    bool can_close() {
      if (lru_is_expireable()) {
	assert(data.empty());
	assert(waitfor_commit.empty());
	return true;
      }
      return false;
    }

    /**
     * Check buffers and waiters for consistency
     * - no overlapping buffers
     * - index in map matches BH
     * - waiters fall within BH
     */
    void audit_buffers();

    /**
     * find first buffer that includes or follows an offset
     *
     * @param offset object byte offset
     * @return iterator pointing to buffer, or data.end()
     */
    map<loff_t,BufferHead*>::iterator data_lower_bound(loff_t offset) {
      map<loff_t,BufferHead*>::iterator p = data.lower_bound(offset);
      if (p != data.begin() &&
	  (p == data.end() || p->first > offset)) {
	--p;     // might overlap!
	if (p->first + p->second->length() <= offset)
	  ++p;   // doesn't overlap.
      }
      return p;
    }

    // bh
    // add to my map
    void add_bh(BufferHead *bh) {
      if (data.empty())
	get();
      assert(data.count(bh->start()) == 0);
      data[bh->start()] = bh;
    }
    void remove_bh(BufferHead *bh) {
      assert(data.count(bh->start()));
      data.erase(bh->start());
      if (data.empty())
	put();
    }

    bool is_empty() { return data.empty(); }

    // mid-level
    BufferHead *split(BufferHead *bh, loff_t off);
    void merge_left(BufferHead *left, BufferHead *right);
    void try_merge_bh(BufferHead *bh);

    bool is_cached(loff_t off, loff_t len);
    bool include_all_cached_data(loff_t off, loff_t len);
    int map_read(ObjectExtent &ex,
                 map<loff_t, BufferHead*>& hits,
                 map<loff_t, BufferHead*>& missing,
                 map<loff_t, BufferHead*>& rx,
		 map<loff_t, BufferHead*>& errors, bool external_call = true);
    BufferHead *map_write(ObjectExtent &ex, ceph_tid_t tid);
    
    void replace_journal_tid(BufferHead *bh, ceph_tid_t tid);
    void truncate(loff_t s);
    void discard(loff_t off, loff_t len);

    // reference counting
    int get() {
      assert(ref >= 0);
      if (ref == 0) lru_pin();
      return ++ref;
    }
    int put() {
      assert(ref > 0);
      if (ref == 1) lru_unpin();
      --ref;
      return ref;
    }
  };


  struct ObjectSet {
    void *parent;

    inodeno_t ino;
    uint64_t truncate_seq, truncate_size;

    int64_t poolid;
    xlist<Object*> objects;

    int dirty_or_tx;
    bool return_enoent;

    ObjectSet(void *p, int64_t _poolid, inodeno_t i)
      : parent(p), ino(i), truncate_seq(0),
	truncate_size(0), poolid(_poolid), dirty_or_tx(0),
	return_enoent(false) {}

  };


  // ******* ObjectCacher *********
  // ObjectCacher fields
 private:
  WritebackHandler& writeback_handler;
  bool scattered_write;

  string name;
  Mutex& lock;

  uint64_t max_dirty, target_dirty, max_size, max_objects;
  ceph::timespan max_dirty_age;
  bool block_writes_upfront;

  //Thomas added: for local cache only
  bool local_cache, ro_cache;
  LCache *lcache;

  flush_set_callback_t flush_set_callback;
  void *flush_set_callback_arg;

  // indexed by pool_id
  vector<ceph::unordered_map<sobject_t, Object*> > objects;

  list<Context*> waitfor_read;

  ceph_tid_t last_read_tid;

  set<BufferHead*, BufferHead::ptr_lt> dirty_or_tx_bh;
  LRU   bh_lru_dirty, bh_lru_rest;
  LRU   ob_lru;

  Cond flusher_cond;
  bool flusher_stop;
  void flusher_entry();
  class FlusherThread : public Thread {
    ObjectCacher *oc;
  public:
    explicit FlusherThread(ObjectCacher *o) : oc(o) {}
    void *entry() {
      oc->flusher_entry();
      return 0;
    }
  } flusher_thread;

  Finisher finisher;

  // objects
  Object *get_object_maybe(sobject_t oid, object_locator_t &l) {
    // have it?
    if (((uint32_t)l.pool < objects.size()) &&
	(objects[l.pool].count(oid)))
      return objects[l.pool][oid];
    return NULL;
  }

  Object *get_object(sobject_t oid, uint64_t object_no, ObjectSet *oset,
		     object_locator_t &l, uint64_t truncate_size,
		     uint64_t truncate_seq);
  void close_object(Object *ob);

  // bh stats
  Cond  stat_cond;

  loff_t stat_clean;
  loff_t stat_zero;
  loff_t stat_dirty;
  loff_t stat_rx;
  loff_t stat_tx;
  loff_t stat_missing;
  loff_t stat_error;
  loff_t stat_dirty_waiting;   // bytes that writers are waiting on to write

  void verify_stats() const;

  void bh_stat_add(BufferHead *bh);
  void bh_stat_sub(BufferHead *bh);
  loff_t get_stat_tx() { return stat_tx; }
  loff_t get_stat_rx() { return stat_rx; }
  loff_t get_stat_dirty() { return stat_dirty; }
  loff_t get_stat_dirty_waiting() { return stat_dirty_waiting; }
  loff_t get_stat_clean() { return stat_clean; }
  loff_t get_stat_zero() { return stat_zero; }

  void touch_bh(BufferHead *bh) {
    if (bh->is_dirty())
      bh_lru_dirty.lru_touch(bh);
    else
      bh_lru_rest.lru_touch(bh);

    bh->set_dontneed(false);
    bh->set_nocache(false);
    touch_ob(bh->ob);
  }
  void touch_ob(Object *ob) {
    ob_lru.lru_touch(ob);
  }
  void bottouch_ob(Object *ob) {
    ob_lru.lru_bottouch(ob);
  }

  // bh states
  void bh_set_state(BufferHead *bh, int s);
  void copy_bh_state(BufferHead *bh1, BufferHead *bh2) {
    bh_set_state(bh2, bh1->get_state());
  }

  void mark_missing(BufferHead *bh, bool update = true);
  void mark_clean(BufferHead *bh, bool update = true);
  void mark_zero(BufferHead *bh, bool update = true);
  void mark_rx(BufferHead *bh, bool update = true);
  void mark_tx(BufferHead *bh, bool update = true);
  void mark_error(BufferHead *bh, bool update = true);
  void mark_dirty(BufferHead *bh, bool update = true);

  void bh_add(Object *ob, BufferHead *bh);
  void bh_remove(Object *ob, BufferHead *bh);

  // io
  void bh_read(BufferHead *bh, int op_flags);
  void bh_write(BufferHead *bh);
  void bh_write_scattered(list<BufferHead*>& blist);
  void bh_write_adjacencies(BufferHead *bh, ceph::real_time cutoff,
			    int64_t *amount, int *max_count);

  void trim();
  void flush(loff_t amount=0);

  /**
   * flush a range of buffers
   *
   * Flush any buffers that intersect the specified extent.  If len==0,
   * flush *all* buffers for the object.
   *
   * @param o object
   * @param off start offset
   * @param len extent length, or 0 for entire object
   * @return true if object was already clean/flushed.
   */
  bool flush(Object *o, loff_t off, loff_t len);
  loff_t release(Object *o);
  void purge(Object *o);

  int64_t reads_outstanding;
  Cond read_cond;

  int _readx(OSDRead *rd, ObjectSet *oset, Context *onfinish,
	     bool external_call);
  void retry_waiting_reads();

 public:
  void bh_read_finish(int64_t poolid, sobject_t oid, ceph_tid_t tid,
		      loff_t offset, uint64_t length,
		      bufferlist &bl, int r,
		      bool trust_enoent);
  void bh_write_commit(int64_t poolid, sobject_t oid,
		       vector<pair<loff_t, uint64_t> >& ranges,
		       ceph_tid_t t, int r);


  class C_ReadFinish : public Context {
    ObjectCacher *oc;
    int64_t poolid;
    sobject_t oid;
    loff_t start;
    uint64_t length;
    xlist<C_ReadFinish*>::item set_item;
    bool trust_enoent;
    ceph_tid_t tid;

  public:
    bufferlist bl;
    C_ReadFinish(ObjectCacher *c, Object *ob, ceph_tid_t t, loff_t s,
		 uint64_t l) :
      oc(c), poolid(ob->oloc.pool), oid(ob->get_soid()), start(s), length(l),
      set_item(this), trust_enoent(true),
      tid(t) {
      ob->reads.push_back(&set_item);
    }

    void finish(int r) {
      oc->bh_read_finish(poolid, oid, tid, start, length, bl, r, trust_enoent);

      // object destructor clears the list
      if (set_item.is_on_list())
	set_item.remove_myself();
    }

    void distrust_enoent() {
      trust_enoent = false;
    }
  };

  class C_WriteCommit : public Context {
    ObjectCacher *oc;
    int64_t poolid;
    sobject_t oid;
    vector<pair<loff_t, uint64_t> > ranges;
  public:
    ceph_tid_t tid;
    C_WriteCommit(ObjectCacher *c, int64_t _poolid, sobject_t o, loff_t s,
		  uint64_t l) :
      oc(c), poolid(_poolid), oid(o), tid(0) {
	ranges.push_back(make_pair(s, l));
      }
    C_WriteCommit(ObjectCacher *c, int64_t _poolid, sobject_t o,
		  vector<pair<loff_t, uint64_t> >& _ranges) :
      oc(c), poolid(_poolid), oid(o), tid(0) {
	ranges.swap(_ranges);
      }
    void finish(int r) {
      oc->bh_write_commit(poolid, oid, ranges, tid, r);
    }
 };

  class C_WaitForWrite : public Context {
  public:
    C_WaitForWrite(ObjectCacher *oc, uint64_t len, Context *onfinish) :
      m_oc(oc), m_len(len), m_onfinish(onfinish) {}
    void finish(int r);
  private:
    ObjectCacher *m_oc;
    uint64_t m_len;
    Context *m_onfinish;
  };

  void perf_start();
  void perf_stop();



  ObjectCacher(CephContext *cct_, string name, WritebackHandler& wb, Mutex& l,
	       flush_set_callback_t flush_callback,
	       void *flush_callback_arg,
	       uint64_t max_bytes, uint64_t max_objects,
	       uint64_t max_dirty, uint64_t target_dirty, double max_age,
	       bool block_writes_upfront);
  ~ObjectCacher();

  void start() {
    flusher_thread.create("flusher");
  }

  void stop() ;

  class C_RetryRead : public Context {
    ObjectCacher *oc;
    OSDRead *rd;
    ObjectSet *oset;
    Context *onfinish;
  public:
    C_RetryRead(ObjectCacher *_oc, OSDRead *r, ObjectSet *os, Context *c)
      : oc(_oc), rd(r), oset(os), onfinish(c) {}
    void finish(int r) {
      if (r < 0) {
	if (onfinish)
	  onfinish->complete(r);
	return;
      }
      int ret = oc->_readx(rd, oset, onfinish, false);
      if (ret != 0 && onfinish) {
	onfinish->complete(ret);
      }
    }
  };



  // non-blocking.  async.

  /**
   * @note total read size must be <= INT_MAX, since
   * the return value is total bytes read
   */
  int readx(OSDRead *rd, ObjectSet *oset, Context *onfinish);
  int writex(OSDWrite *wr, ObjectSet *oset, Context *onfreespace);
  bool is_cached(ObjectSet *oset, vector<ObjectExtent>& extents,
		 snapid_t snapid);

private:
  // write blocking
  int _wait_for_write(OSDWrite *wr, uint64_t len, ObjectSet *oset,
		      Context *onfreespace);
  void maybe_wait_for_writeback(uint64_t len);
  bool _flush_set_finish(C_GatherBuilder *gather, Context *onfinish);

public:
  bool is_enable_cache() { return local_cache; }
  bool is_rw_cache() { return local_cache && !ro_cache; }
  bool is_ro_cache() { return local_cache && ro_cache; }
  bool can_merge_bh(BufferHead *left, BufferHead *right);
  bool idle() {
	return ((uint64_t)get_stat_dirty() > 0 && 
	 	(uint64_t)get_stat_dirty() < target_dirty);
  }
  
  void init_cache(uint64_t cache_size, uint32_t obj_order, const std::string &obj_prefix, 
  	bool old_format, ObjectSet *oset, const file_layout_t &l);

  bool set_is_empty(ObjectSet *oset);
  bool set_is_cached(ObjectSet *oset);
  bool set_is_dirty_or_committing(ObjectSet *oset);

  bool flush_set(ObjectSet *oset, Context *onfinish=0);
  bool flush_set(ObjectSet *oset, vector<ObjectExtent>& ex,
		 Context *onfinish = 0);
  bool flush_all(Context *onfinish = 0);

  void purge_set(ObjectSet *oset);

  // returns # of bytes not released (ie non-clean)
  loff_t release_set(ObjectSet *oset);
  uint64_t release_all();

  void discard_set(ObjectSet *oset, const vector<ObjectExtent>& ex);

  /**
   * Retry any in-flight reads that get -ENOENT instead of marking
   * them zero, and get rid of any cached -ENOENTs.
   * After this is called and the cache's lock is unlocked,
   * any new requests will treat -ENOENT normally.
   */
  void clear_nonexistence(ObjectSet *oset);


  // cache sizes
  void set_max_dirty(uint64_t v) {
    max_dirty = v;
  }
  void set_target_dirty(int64_t v) {
    target_dirty = v;
  }
  void set_max_size(int64_t v) {
    max_size = v;
  }
  void set_max_dirty_age(double a) {
    max_dirty_age = make_timespan(a);
  }
  void set_max_objects(int64_t v) {
    max_objects = v;
  }


  // file functions

  /*** async+caching (non-blocking) file interface ***/
  int file_is_cached(ObjectSet *oset, file_layout_t *layout,
		     snapid_t snapid, loff_t offset, uint64_t len) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, extents);
    return is_cached(oset, extents, snapid);
  }

  int file_read(ObjectSet *oset, file_layout_t *layout, snapid_t snapid,
		loff_t offset, uint64_t len, bufferlist *bl, int flags,
		Context *onfinish) {
    OSDRead *rd = prepare_read(snapid, bl, flags);
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, rd->extents);
    return readx(rd, oset, onfinish);
  }

  int file_write(ObjectSet *oset, file_layout_t *layout,
		 const SnapContext& snapc, loff_t offset, uint64_t len,
		 bufferlist& bl, ceph::real_time mtime, int flags) {
    OSDWrite *wr = prepare_write(snapc, bl, mtime, flags, 0);
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, wr->extents);
    return writex(wr, oset, NULL);
  }

  bool file_flush(ObjectSet *oset, file_layout_t *layout,
		  const SnapContext& snapc, loff_t offset, uint64_t len,
		  Context *onfinish) {
    vector<ObjectExtent> extents;
    Striper::file_to_extents(cct, oset->ino, layout, offset, len,
			     oset->truncate_size, extents);
    return flush_set(oset, extents, onfinish);
  }
};


inline ostream& operator<<(ostream& out, ObjectCacher::BufferHead &bh)
{
  out << "bh[ " << &bh << " "
      << bh.start() << "~" << bh.length()
      << " " << bh.ob
      << " (" << bh.bl.length() << ")"
      << " v " << bh.last_write_tid;
  if (bh.get_journal_tid() != 0) {
    out << " j " << bh.get_journal_tid();
  }
  if (bh.is_tx()) out << " tx";
  if (bh.is_rx()) out << " rx";
  if (bh.is_dirty()) out << " dirty";
  if (bh.is_clean()) out << " clean";
  if (bh.is_zero()) out << " zero";
  if (bh.is_missing()) out << " missing";
  if (bh.bl.length() > 0) out << " firstbyte=" << (int)bh.bl[0];
  if (bh.error) out << " error=" << bh.error;
  out << "]";
  out << " waiters = {";
  for (map<loff_t, list<Context*> >::const_iterator it
	 = bh.waitfor_read.begin();
       it != bh.waitfor_read.end(); ++it) {
    out << " " << it->first << "->[";
    for (list<Context*>::const_iterator lit = it->second.begin();
	 lit != it->second.end(); ++lit) {
	 out << *lit << ", ";
    }
    out << "]";
  }
  out << "}";
  return out;
}

inline ostream& operator<<(ostream& out, ObjectCacher::ObjectSet &os)
{
  return out << "objectset[" << os.ino
	     << " ts " << os.truncate_seq << "/" << os.truncate_size
	     << " objects " << os.objects.size()
	     << " dirty_or_tx " << os.dirty_or_tx
	     << "]";
}

inline ostream& operator<<(ostream& out, ObjectCacher::Object &ob)
{
  out << "object["
      << ob.get_soid() << " oset " << ob.oset << dec
      << " wr " << ob.last_write_tid << "/" << ob.last_commit_tid;

  if (ob.complete)
    out << " COMPLETE";
  if (!ob.exists)
    out << " !EXISTS";

  out << "]";
  return out;
}

/**************************************************************************
  Author: Thomas.lee
  Purpose: local cache
  Date: 2016/10/8
***************************************************************************/
class LCache {
public:
  #define VER_LEN  16
  #define FILE_VER "2016.10.8"
  #define CHAR_BITS_SHIFT 		(3)
  #define FILE_HEAD_ALIAN_SIZE  (512)
  #define BITMAP_CHUNK_SIZE (4*1024) 	//4KB
  #define MAX_OBJECTS		(1*1024) 	//1k
  #define MAX_DIFRTY_RATIO 	 (0.8)
  #define TARGET_DIRTY_RATIO (0.5)
  #define OBJ_ORDER		(22)		//4MB
  #define CHUNK_ORDER	(13)		//8kB
  
  // ******* FileHead *********
  struct FileHead {
	char ver[VER_LEN];		//version = FILE_VER
	uint32_t size;			//size, in byte, of this structure, including bitmap size
	uint64_t file_size;		//size, in byte, of cache file, including overhead
	loff_t oft;				//in-file offset, where itself located in
	loff_t obj_meta_offset;	//offset, in byte, of the first ObjMeta location
	loff_t data_head_offset;//offset, in byte, of DataHead 
	uint32_t obj_meta_size;	//size, in byte, of a ObjectMeta
	uint32_t data_head_size; //size, in byte, of DataHead,including bitmap size
	uint32_t max;			//maxium number of ObjectMetas in file
	uint32_t cur;			//current account of ObjectMetas in file 
	uint8_t obj_order;		//size, in byte, of a Object, default is 4MB
	uint8_t chunk_order;	//default is 1MB, which means each OBJ is divided into 4 parts
	char 	bitmap[];		//elastic array member for ObjMeta
  }__attribute__((__packed__));
  #define FILE_HEAD_SIZE	(sizeof(struct FileHead))


  //******* DataHead *********
  struct DataHead {
	uint32_t size;			//size, in byte, of this structure, including bitmap
	uint32_t meta_size;		//size, in byte of a BufMeta
	loff_t oft;				//offset, in byte, of this structure, equal to FileHead::data_head_offset
	loff_t chk_oft;			//base offset of data chk(oft+size)
	uint32_t max;			//maxium number of BufMetas in file
	uint32_t cur;			//current used account of BufMetas in file
	char bitmap[];			//elastic array member for BufMeta
  }__attribute__((__packed__));
  #define DATA_HEAD_SIZE (sizeof(struct DataHead))

  // ******* ObjMeta ***********
  struct ObjMeta {
	uint64_t oid;				//object id		
	uint64_t truncate_size;
	uint64_t truncate_seq;
	int64_t pid;				//pool id
	loff_t oft;					//absolute offset, in byte, of this structure in  file
	loff_t data_l;				//point to the first BufMeta, all BufMeta linked into an ordered list
	ceph_tid_t last_write_tid;  //version of bh (if non-zero)
    ceph_tid_t last_commit_tid; //last update commited.
    ceph_timespec l_access;		//last update / access time				
    bool complete;			    //read op is completed
    bool exists;				//for read op
  }__attribute__ ((__packed__));
  #define OBJ_META_SIZE (sizeof(struct ObjMeta))
  
  // ******* BufMeta ***********
  struct BufMeta {
	uint64_t oid;				//object id
	loff_t oft;					//absolute offset, in byte, of this structure
	struct {
	  loff_t prev, next;	    //pointer to previous & next BufferMeta, default -1;
	} mb;
	struct {
      loff_t start, length;     //bh extent in object
    } ex;
	ceph_tid_t last_write_tid;  //version of bh (if non-zero)
    ceph_tid_t last_read_tid;   //tid of last read op (if any)
	ceph_timespec l_write;		//last write time / access time
	int stat;					//state
	int error; 					//holds return value for failed reads
  }__attribute__ ((__packed__));
  #define BUF_META_SIZE (sizeof(struct BufMeta))

public:
  LCache(ObjectCacher *_oc, Mutex &l, uint64_t cache_size, 
  	uint64_t max_dirty, uint64_t target_dirty, 
  	double dirty_age, bool ro, std::string cache_path) 
  	: fd(0), oc(_oc), cache_lock(l)
  	, image_size(cache_size*20), cache_size(cache_size)
  	, max_dirty(max_dirty), target_dirty(target_dirty), max_objects(MAX_OBJECTS)
    , cache_hw(MAX_DIFRTY_RATIO), cache_lw(TARGET_DIRTY_RATIO), obj_order(OBJ_ORDER), chunk_order(CHUNK_ORDER)
    , last_ios(0), io_freq(0), idle_tick(0), cache_age(ceph::make_timespan(dirty_age))
  	, file_head(NULL), data_head(NULL), object_format(NULL), read_only(ro)
  	, cache_file_path(cache_path), fh(NULL), dh(NULL)
  	, reclaim_stat_waiting(0), reclaim_stop(false), reclaim_thread(this) {

   	set_cache_wm(max_dirty, target_dirty);
  }

  virtual ~LCache() {
  	close();
	
	if ( NULL != file_head )
		delete [] file_head;
	fh = NULL;
	file_head = NULL;

	if (NULL != data_head )
		delete [] data_head;
	dh = NULL;
	data_head = NULL;

	if ( NULL != object_format )
		delete [] object_format;
	object_format = NULL;
  }

  int start(ObjectCacher::ObjectSet *oset, const file_layout_t &l);

  loff_t add_objmeta(ObjectCacher::Object *o);
  int update_objmeta(ObjectCacher::Object *o);
  int load_update_objmeta(ObjectCacher::Object *o);

  int add_bufmeta(ObjectCacher::BufferHead *bh, loff_t oft, loff_t len);
  int update_bufmeta(ObjectCacher::BufferHead* bh);
  int load_bufmeta(ObjectCacher::Object *o, loff_t offset, loff_t len);

  int add_chunk(ObjectCacher::BufferHead* bh);
  int get_chunk(ObjectCacher::BufferHead* bh, loff_t oft, loff_t len, bufferlist &bl);

  void set_image_size(uint64_t size) { image_size = size; }
  uint64_t get_image_size() { return image_size; };

  void set_cache_size(uint64_t size) { cache_size = size; }
  uint64_t get_cache_size() { return cache_size; }
  
  void set_cache_path(const std::string& path) { cache_file_path = path; }
  const std::string cache_path() { return cache_file_path; }
  
  void set_obj_order(uint32_t order) { obj_order = order; }
  uint32_t get_obj_order() { return obj_order; }
  
  void set_chk_order(uint32_t order) { 
	chunk_order = order;
	if ( chunk_order == 0 
		|| chunk_order > obj_order )
		chunk_order = obj_order;
  }
  uint32_t get_chk_order() { return chunk_order; }
  
  void set_max_objects(uint64_t v) { max_objects = v; }
  uint64_t get_max_objects() { return max_objects; }
  
  void set_cache_wm(uint64_t hw, uint64_t lw) {
	if ( hw >= cache_size || hw == 0 )
	  cache_hw = MAX_DIFRTY_RATIO;
	else
	  cache_hw = (float)hw/cache_size;
  
	if ( lw > hw || lw >= cache_size || lw == 0 )
	  cache_lw = TARGET_DIRTY_RATIO;
	else
	  cache_lw = (float)lw/cache_size;

	max_dirty =  cache_size*cache_hw;
	target_dirty = cache_size*cache_lw;
  }
  
  float get_cache_hw() { return cache_hw; }
  float get_cache_lw() { return cache_lw; }
  uint64_t get_max_dirty() { return max_dirty; }
  uint64_t get_target_dirty() { return target_dirty; }

  bool is_lw() {
	if ( fh->cur > fh->max*cache_lw
		|| dh->cur > dh->max*cache_lw )
		return true;
	return false;
  }

  bool is_hw () {
	if ( fh->cur > fh->max*cache_hw
		|| dh->cur > dh->max*cache_hw )
		return true;
	return false;
  }

  void reclaim_sig() { reclaim_cond.Signal(); }
  void reclaim_stat_wait() { reclaim_stat_cond.Wait(cache_lock); }
  void reclaim_stat_waiting_add(loff_t len) { reclaim_stat_waiting +=len; }
  void reclaim_stat_waiting_sub(loff_t len) { reclaim_stat_waiting -=len; }

  void stop() {
	if ( reclaim_thread.is_started() ) {
		cache_lock.Lock();  // hmm.. watch out for deadlock!
		reclaim_stop = true;
		reclaim_cond.Signal();
		cache_lock.Unlock();
		reclaim_thread.join();
	}
  }

  void set_obj_prefix(const std::string &obj_prefix, bool old_format) {
    object_prefix = obj_prefix;
    size_t len = object_prefix.length() + 16;
    object_format = new char[len];
	if (old_format) {
	  snprintf(object_format, len, "%s.%%012llx", object_prefix.c_str());
	} else {
	  snprintf(object_format, len, "%s.%%016llx", object_prefix.c_str());
	}
  }
  
  uint64_t oid_to_no(const std::string &oid) {
	istringstream iss(oid);
    // skip object prefix and separator
	iss.ignore(object_prefix.length() + 1);
	uint64_t num;
	iss >> std::hex >> num;
	return num; 
  }

 private:
  int open(const std::string path, int flags = O_RDWR); 
  int load(ObjectCacher::ObjectSet *oset, const file_layout_t &l);

  int read(void* buf, uint32_t len, loff_t pos, int w = SEEK_SET) {
    if ( NULL == buf || fd <= 0 )
	  return -1;
    int r = lseek(fd, pos, w);
    if ( r == -1 ) {
	  lderr(oc->cct) << "Failed to seek cache file " << cpp_strerror(r) << dendl;
	  return r;
    }

    return ::read(fd, buf, len);
  }
  
  int write(void* buf, uint32_t len, loff_t pos, int w = SEEK_SET) {
    if ( NULL == buf || fd <= 0 )
	  return -1;

    int r = lseek(fd, pos, w);
    if ( r == -1 ) {
	  lderr(oc->cct) << "Failed to seek cache file " << cpp_strerror(r) << dendl;
	  return r;
    }

    return ::write(fd, buf, len);
  }
  
  int unlink() {
    return ::unlink(cache_file_path.c_str());
  }
  
  int close() {
    if ( fd > 0 )
  	  ::close(fd);
    fd = 0;
	return 0;
  }

  void init_objmeta(ObjectCacher::Object *o, ObjMeta &om);

  /*get ObjMeta from local cache file
  
   return -1 if not exist.
  */
  int get_objmeta(loff_t offset, ObjMeta &om);
  int get_objmeta(uint64_t oid, ObjMeta &om);

  //insert om into cache file if it doesn't exist
  loff_t insert_objmeta(ObjMeta &om);

  /*check whether there is a ObjMeta in postion oft

   return true if exist
  */
  bool is_exist(loff_t oft) {
	if ( oft < fh->obj_meta_offset 
		|| oft >= fh->data_head_offset )
	  return false;

	uint32_t pos = (oft - fh->obj_meta_offset)/fh->obj_meta_size;
	if ( obj_map.test_bit(pos) )
	  return true;

	return false;
  }

  /*update ObjMeta in local cache file

   return -1 if not exist
  */
  int update_objmeta(ObjMeta &om);

  bool is_removable(ObjMeta &om) {
    if ( om.data_l == -1 )
	  return true;
	return false;
  }

  int clear_objmeta(ObjMeta &om) {
  	int r;
	uint32_t pos = (om.oft - fh->obj_meta_offset)/fh->obj_meta_size;
	obj_map.free_bit(pos);

	//update FileHead
	--fh->cur;
	r = write(file_head, FILE_HEAD_ALIAN_SIZE, fh->oft);
	assert(r == FILE_HEAD_ALIAN_SIZE);

	uint32_t oft = FILE_HEAD_ALIAN_SIZE + (pos>>CHAR_BITS_SHIFT);
	uint32_t size = (fh->size - oft) > BITMAP_CHUNK_SIZE? BITMAP_CHUNK_SIZE: (fh->size - oft);
	r = write(file_head + oft, size, fh->oft + oft);
	assert(r == (int)size);

	del_mem_head(om.oid);
	del_mem_object(om);
	return r;
  }
  
  /*remove ObjMeta and its relevant Object in memory
   return -1 if failed
  */
  int remove_objmeta(ObjMeta &om);
  
  void init_bufmeta (ObjectCacher::BufferHead *bh, BufMeta &bm);
  loff_t insert_bufmeta(BufMeta &bm);

  /*get BufMeta from cache file(excluding file data)

   return -1 if failed
  */
  int get_bufmeta(loff_t offset, BufMeta &bm);

  /*update BufMeta in cache file

   return -1 if failed
  */
  int update_bufmeta(BufMeta &bm);
	
  /*remove STATE_CLEAN/STATE_ZERO BufMeta and delete BufferHead

   return -1 if failed
  */
  int free_clean_bufmeta(ObjMeta &om, BufMeta &bm, map<loff_t, BufMeta*>::iterator &p);
  int free_bufmeta(ObjMeta &om, BufMeta &bm, map<loff_t, BufMeta*>::iterator &p);
  int merge_left(BufMeta* bm, loff_t cur, loff_t len);
  int merge_right(map<loff_t, BufMeta*>::iterator &p, BufMeta *bm);
  
  int clear_bufmeta(BufMeta &bm) {
  	int r;
	uint32_t pos = (bm.oft - dh->chk_oft)/(dh->meta_size+(1<<fh->chunk_order));
	buf_map.free_bit(pos);

	//update DataHead
	--dh->cur;
	r = write(data_head, FILE_HEAD_ALIAN_SIZE, dh->oft);
	assert(r == FILE_HEAD_ALIAN_SIZE);

    //update bitmap
    uint32_t oft = FILE_HEAD_ALIAN_SIZE + (pos>>CHAR_BITS_SHIFT);
    uint32_t size = (dh->size - oft) > BITMAP_CHUNK_SIZE? BITMAP_CHUNK_SIZE: (dh->size - oft);
	r = write(data_head + oft, size, dh->oft + oft);
	assert(r == (int)size);
	
	del_mem_data(bm);	
	return r;
  }

  int add_mem_data(BufMeta &bm) {
	assert(!data[bm.oid].count(bm.ex.start));
	BufMeta *pm = new BufMeta;
	
	memcpy((char*)pm, (char*)&bm, sizeof(bm));
	data[pm->oid][pm->ex.start] = pm;

	return 0;
  }

  int del_mem_data(BufMeta &bm) {
	map<loff_t, BufMeta*>::iterator p = get_mem_data(bm);
	if ( p != data[bm.oid].end() ) {
		data[bm.oid].erase(bm.ex.start);
	    delete p->second;
		p->second = NULL;
	}
	return 0;
  }

  int update_mem_data(BufMeta &bm) {
  	if ( data.count(bm.oid) && data[bm.oid].count(bm.ex.start) ) 
		memcpy((char*)data[bm.oid][bm.ex.start], (char*)&bm, sizeof(bm));
	
	return 0;
  }

  map<loff_t, BufMeta*>::iterator replace_mem_data(loff_t old_start, BufMeta &bm) {
   	assert(data.count(bm.oid));
	assert(data[bm.oid].count(old_start));

	BufMeta *p = data[bm.oid][old_start];
	data[bm.oid].erase(old_start);
	data[bm.oid][bm.ex.start] = p;
	return get_mem_data(bm);
  }

  map<loff_t, BufMeta*>::iterator get_mem_head(uint64_t oid) {
	if ( !data.count(oid) )
		return data[oid].end();
	return data[oid].begin();
  }

  map<loff_t, BufMeta*>::iterator get_mem_tail(uint64_t oid) {
  	return data[oid].end(); 
  }
  
  int del_mem_head(uint64_t oid) {
	if ( data.count(oid) )
		data.erase(oid);
	return 0;
  }
  
  map<loff_t, BufMeta*>::iterator get_mem_data(BufMeta &bm) {
  	if ( ! data.count(bm.oid) )
		return data[bm.oid].end();
	
	map<loff_t,BufMeta*>::iterator p = data[bm.oid].lower_bound(bm.ex.start);
    if (p != data[bm.oid].begin() &&
	  (p == data[bm.oid].end() || p->first > bm.ex.start)) {
		--p;     // might overlap!
		if (p->first + p->second->ex.length <= bm.ex.start)
	  		++p;   // doesn't overlap.
     }
	
     return p;	
  }
  
  map<loff_t, BufMeta*>::iterator get_mem_data(uint64_t oid, loff_t offset) {
    if ( ! data.count(oid) )
		return data[oid].end();
	
	map<loff_t,BufMeta*>::iterator p = data[oid].lower_bound(offset);
    if (p != data[oid].begin() &&
	  (p == data[oid].end() || p->first > offset)) {
		--p;     // might overlap!
		if (p->first + p->second->ex.length <= offset)
	  		++p;   // doesn't overlap.
     }
	
     return p;	
  }

  int add_mem_object(ObjMeta &om) {
  	assert(!objects.count(om.oid));
	
  	ObjMeta *p = new ObjMeta;
	memcpy((char*)p, (char*)&om, sizeof(om));
	objects[om.oid] = p;

	return 0;
  }

  int del_mem_object(ObjMeta &om) {
  	if ( objects.count(om.oid) ) {
		ObjMeta *p = objects[om.oid];

		objects.erase(om.oid);
		delete p;
		p = NULL;
  	}

	return 0;
  }

  int update_mem_object(ObjMeta &om) {
	if ( objects.count(om.oid) ) 
		memcpy((char*)objects[om.oid], (char*)&om, sizeof(om));
	return 0;
  }

  ObjMeta* get_mem_object(uint64_t oid) {
  	if ( objects.count(oid) ) 
		return objects[oid];
	return NULL;
  }

  void evict_mem_object();
  
private:
  int fd;
  ObjectCacher *oc;
  Mutex& cache_lock;
  uint64_t image_size, cache_size;
  uint64_t max_dirty, target_dirty, max_objects;
  float cache_hw, cache_lw;
  uint32_t obj_order, chunk_order;
  uint32_t last_ios, io_freq, idle_tick;
  ceph::timespan cache_age;
  char *file_head, *data_head, *object_format;
  bool read_only;
  std::string cache_file_path;
  std::string object_prefix;
  FileHead *fh;
  DataHead *dh;

  ceph::unordered_map<uint64_t, map<loff_t, BufMeta*> > data;
  ceph::unordered_map<uint64_t, ObjMeta*> objects;

  //********* BitMap ******************
  class BitMap {
  public:
	BitMap() 
		: map_chunk(BITMAP_CHUNK_SIZE), map_c(0),
		t_size(0), t_bits(0), t_free(0), map(NULL) {	
       map_meta.clear();
	}
	
	~BitMap() {
		map = NULL;
	}

	void init_map(uint32_t chk_size, uint32_t map_size, 
		uint32_t max_bits, uint32_t used_bits) {
		map_chunk = chk_size;
		t_size = map_size;
		t_bits = max_bits;
		t_free = t_bits - used_bits;
	}

	void set_map(void* _map, uint32_t size) {
	  assert(size == t_size);
	  
	  uint32_t used = t_bits - t_free;
      map_c = (t_size + map_chunk - 1) / map_chunk;
      map_meta.resize(map_c);
	  map = (char*)_map;

	  map_c = 0;
	  while ( size > 0 ) {
		uint32_t chunk = (size > map_chunk)? map_chunk: size;

		map_meta[map_c].size = chunk;
		map_meta[map_c].free = chunk<<(CHAR_BITS_SHIFT);
		map_meta[map_c].start = (map_c*map_chunk)<<(CHAR_BITS_SHIFT);
		map_meta[map_c].end = map_meta[map_c].start + map_meta[map_c].free;

		for (uint32_t i = map_meta[map_c].start; 
			i < map_meta[map_c].end && used > 0; 
			++i) {
			if (test_bit(i)) {
				--used;
				--map_meta[map_c].free;
			}
		}
		++map_c;
		size -= chunk;
	  }
	}

	char* get_map() { return map; }
	uint32_t get_size() { return t_size; }
	uint32_t get_bits() { return t_bits; }
	uint32_t get_maps() { return map_c; }

	bool test_bit(uint32_t n) {
	  return ((map[n>>(CHAR_BITS_SHIFT)]) & (1<<(n&7))) != 0;
	}

	void set_bit(uint32_t n) {
	  map[n>>(CHAR_BITS_SHIFT)] |= (1<<(n&7));
	}

	uint32_t try_get_pos(uint64_t oid) {
		uint32_t pos = oid;
		if ( oid >= t_bits ) {
			oid = oid % t_bits;
			pos = (oid / (map_chunk<<CHAR_BITS_SHIFT));
			pos = (oid % map_meta[pos].size<<(CHAR_BITS_SHIFT)) + map_meta[pos].start;
		}
		
		return pos;
	}
	
	uint32_t test_set_bit(uint64_t oid) {
	  uint32_t slot, pos;

	  pos = (oid >= t_bits)? (oid % t_bits): oid;
	  slot = pos = (pos / (map_chunk<<CHAR_BITS_SHIFT));
	  
	  while (slot < map_c && map_meta[slot].free == 0) ++slot;
	  if (slot == map_c) {
		slot = pos - 1;
		while (slot < pos && map_meta[slot].free == 0) --slot;
	  }
		
	  assert(map_meta[slot].free > 0);
	  --map_meta[slot].free;
	  --t_free;

	  pos = oid;
	  if (oid >= t_bits) {
	  	pos = oid % (map_meta[slot].size<<(CHAR_BITS_SHIFT));
	  	pos += map_meta[slot].start;
	  }
	  
	  for (uint32_t i = pos; i < map_meta[slot].end; ++i) {
		if (!test_bit(i)) {
		  set_bit(i);
		  return i;
		}
	  }

	  for (uint32_t i = map_meta[slot].start; i < pos; ++i) {
	  	if (!test_bit(i)) {
		  set_bit(i);
		  return i;
	  	}
	  }

	  return t_bits;
	}

	void free_bit(uint32_t n) {
	  uint32_t slot = n / (map_chunk<<(CHAR_BITS_SHIFT));
	  if ( n >= map_meta[slot].end )
	  	++slot;

	  assert(slot < map_c);
	  ++map_meta[slot].free;
	  ++t_free;
      map[n>>(CHAR_BITS_SHIFT)] &= ~(1<<(n&7));
	}

  private:
  	struct MapMeta {
		uint32_t size;  //size, in byte, of this chunk
		uint32_t free;  //free bits, firstly it equals to size * 8
		uint32_t start; //first bit
		uint32_t end;   //last bit, = start + size * 8
	};
  	uint32_t map_chunk, map_c;
  	uint32_t t_size, t_bits, t_free;
	vector<MapMeta> map_meta; //stats of each map chunk
  	char *map;
  }obj_map, buf_map ;

  //data reclaim thread  
  Cond reclaim_cond;
  Cond reclaim_stat_cond;
  loff_t reclaim_stat_waiting;
  bool reclaim_stop;
  void reclaim_entry();
  class ReClaimThread : public Thread {
	LCache *lc;
  public:
	explicit ReClaimThread(LCache *_lc) : lc(_lc) {}
	void *entry() {
		lc->reclaim_entry();
		return 0;
	}
  } reclaim_thread;
};

inline ostream& operator<<(ostream& out, LCache::ObjMeta &om)
{
  out << " ObjMeta["
      << " oid    = " << om.oid 
      << " pool   = " << om.pid 
      << " oft    = " << om.oft
      << " data_l = " << om.data_l
      << " l_w_tid= " << om.last_write_tid 
      << " l_c_tid= " << om.last_commit_tid
      << "]   ";

  return out;
}

inline ostream& operator<<(ostream& out, LCache::BufMeta &bm)
{
  out << " BufMeta["
  	  << " oid= 	"<<bm.oid
  	  << " oft= 	"<<bm.oft
  	  << " mb   	" << bm.mb.prev << "/" << bm.mb.next
  	  << " ex   	" << bm.ex.start << "~" << bm.ex.length
  	  << " l_r_tid= " << bm.last_read_tid 
  	  << " l_w_tid= " << bm.last_write_tid
  	  << " stat=    "<<bm.stat
  	  << "]"; 
	
  return out;
}

#endif
