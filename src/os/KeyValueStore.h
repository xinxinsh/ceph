// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_KEYVALUESTORE_H
#define CEPH_KEYVALUESTORE_H

#include "include/types.h"

#include <map>
#include <deque>
#include <boost/scoped_ptr.hpp>
#include <fstream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/assert.h"

#include "ObjectStore.h"

#include "common/WorkQueue.h"
#include "common/Finisher.h"

#include "common/Mutex.h"
#include "ObjectMap.h"
#include "SequencerPosition.h"
#include "KeyValueDB.h"

#include "include/uuid.h"

enum kvstore_types {
    KV_TYPE_NONE = 0,
    KV_TYPE_LEVELDB,
    KV_TYPE_OTHER
};

class KeyValueStore : public ObjectStore,
                     public md_config_obs_t
{
public:
  filestore_perf_stat_t get_cur_stats() {
    filestore_perf_stat_t ret;
    return ret;
  }

  static const uint32_t target_version = 1;
private:
  string internal_name;         ///< internal name, used to name the perfcounter instance
  string basedir;
  std::string current_fn;
  std::string current_op_seq_fn;
  std::string omap_dir;
  uuid_d fsid;

  int fsid_fd, op_fd, basedir_fd, current_fd;

  enum kvstore_types kv_type;

  deque<uint64_t> snaps;

  boost::scoped_ptr<KeyValueDB> backend;
  // ObjectMap
  boost::scoped_ptr<ObjectMap> object_map;

  Finisher ondisk_finisher;

  Mutex lock;

  int get_cdir(coll_t, char*, int);

  /// read a uuid from fd
  int read_fsid(int fd, uuid_d *uuid);

  /// lock fsid_fd
  int lock_fsid();

  /// KeyValueStore xattr replacement
  string calculate_key(coll_t cid);
  string calculate_key(coll_t cid, const ghobject_t& oid);
  bool parse_object(const string &long_name, coll_t *coll, ghobject_t *out);

  // -- op workqueue --
  struct Op {
    utime_t start;
    uint64_t op;
    list<Transaction*> tls;
    Context *ondisk, *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
    TrackedOpRef osd_op;
  };
  class OpSequencer : public Sequencer_impl {
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    list<uint64_t> jq;
    Cond cond;
  public:
    Sequencer *parent;
    Mutex apply_lock;  // for apply mutual exclusion

    void queue_journal(uint64_t s) {
      Mutex::Locker l(qlock);
      jq.push_back(s);
    }
    void dequeue_journal() {
      Mutex::Locker l(qlock);
      jq.pop_front();
      cond.Signal();
    }
    void queue(Op *o) {
      Mutex::Locker l(qlock);
      q.push_back(o);
    }
    Op *peek_queue() {
      assert(apply_lock.is_locked());
      return q.front();
    }
    Op *dequeue() {
      assert(apply_lock.is_locked());
      Mutex::Locker l(qlock);
      Op *o = q.front();
      q.pop_front();
      cond.Signal();
      return o;
    }
    void flush() {
      Mutex::Locker l(qlock);

      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
        seq = q.back()->op;
      if (!jq.empty() && jq.back() > seq)
        seq = jq.back();

      if (seq) {
        // everything prior to our watermark to drain through either/both queues
        while ((!q.empty() && q.front()->op <= seq) ||
                (!jq.empty() && jq.front() <= seq))
          cond.Wait(qlock);
      }
    }

    OpSequencer()
      : qlock("KeyValueStore::OpSequencer::qlock", false, false),
	parent(0),
	apply_lock("KeyValueStore::OpSequencer::apply_lock", false, false) {}
    ~OpSequencer() {
      assert(q.empty());
    }

    const string& get_name() const {
      return parent->get_name();
    }
  };

  friend ostream& operator<<(ostream& out, const OpSequencer& s);

  Sequencer default_osr;
  deque<OpSequencer*> op_queue;
  uint64_t op_queue_len, op_queue_bytes;
  Finisher op_finisher;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    KeyValueStore *store;
    OpWQ(KeyValueStore *fs, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<OpSequencer>("KeyValueStore::OpWQ", timeout, suicide_timeout, tp), store(fs) {}

    bool _enqueue(OpSequencer *osr) {
      store->op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) {
      assert(0);
    }
    bool _empty() {
      return store->op_queue.empty();
    }
    OpSequencer *_dequeue() {
      if (store->op_queue.empty())
	return NULL;
      OpSequencer *osr = store->op_queue.front();
      store->op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) {
      store->_do_op(osr, handle);
    }
    void _process_finish(OpSequencer *osr) {
      store->_finish_op(osr);
    }
    void _clear() {
      assert(store->op_queue.empty());
    }
  } op_wq;

  void _do_op(OpSequencer *osr, ThreadPool::TPHandle &handle);
  void _finish_op(OpSequencer *osr);
  Op *build_op(list<Transaction*>& tls, Context *ondisk, Context *onreadable,
               Context *onreadable_sync, TrackedOpRef osd_op);
  void queue_op(OpSequencer *osr, Op *o);

  PerfCounters *logger;

public:
  KeyValueStore(const std::string &base, const std::string &jdev, const char *internal_name = "keyvaluestore", bool update_to=false);
  ~KeyValueStore();

  int _detect_backend() { kv_type = KV_TYPE_LEVELDB; return 0; }
  bool test_mount_in_use();
  int version_stamp_is_valid(uint32_t *version);
  int update_version_stamp();
  int write_version_stamp();
  int read_op_seq(uint64_t *seq);
  int write_op_seq(int, uint64_t seq);
  int mount();
  int umount();
  int get_max_object_name_length();
  int mkfs();
  int mkjournal() {return 0;}

  /**
   ** set_allow_sharded_objects()
   **
   ** Before sharded ghobject_t can be specified this function must be called
   **/
  void set_allow_sharded_objects() {}

  /**
   ** get_allow_sharded_objects()
   **
   ** return value: true if set_allow_sharded_objects() called, otherwise false
   **/
  bool get_allow_sharded_objects() {return false;}

  int statfs(struct statfs *buf);

  int _do_transactions(
    list<Transaction*> &tls, uint64_t op_seq,
    ThreadPool::TPHandle *handle);
  int do_transactions(list<Transaction*> &tls, uint64_t op_seq) {
    return _do_transactions(tls, op_seq, 0);
  }
  unsigned _do_transaction(
    Transaction& t, uint64_t op_seq, int trans_num,
    ThreadPool::TPHandle *handle);

  int queue_transactions(Sequencer *osr, list<Transaction*>& tls,
			 TrackedOpRef op = TrackedOpRef());


  // ------------------
  // objects
  bool exists(coll_t cid, const ghobject_t& oid);
  int stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false);
  int read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false);
  int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);

  int _touch(coll_t cid, const ghobject_t& oid);
  int _write(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl,
      bool replica = false);
  int _zero(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len);
  int _truncate(coll_t cid, const ghobject_t& oid, uint64_t size);
  int _clone(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid,
             const SequencerPosition&);
  int _clone_range(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _do_clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _do_copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _remove(coll_t cid, const ghobject_t& oid);

  int _fgetattr(int fd, const char *name, bufferptr& bp);
  int _fgetattrs(int fd, map<string,bufferptr>& aset, bool user_only);
  int _fsetattrs(int fd, map<string, bufferptr> &aset);

  void _start_sync() {}

  void start_sync() {}
  void start_sync(Context *onsafe) {}
  void sync() {}
  void _flush_op_queue();
  void flush() {}
  void sync_and_flush() {}

  void set_fsid(uuid_d u) {
    fsid = u;
  }
  uuid_d get_fsid() { return fsid; }

  // DEBUG read error injection, an object is removed from both on delete()
  Mutex read_error_lock;
  set<ghobject_t> data_error_set; // read() will return -EIO
  set<ghobject_t> mdata_error_set; // getattr(),stat() will return -EIO
  void inject_data_error(const ghobject_t &oid);
  void inject_mdata_error(const ghobject_t &oid);
  void debug_obj_on_delete(const ghobject_t &oid);
  bool debug_data_eio(const ghobject_t &oid);
  bool debug_mdata_eio(const ghobject_t &oid);

  // attrs
  int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr &bp);
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset, bool user_only = false);

  int _setattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset,
		const SequencerPosition &spos);
  int _rmattr(coll_t cid, const ghobject_t& oid, const char *name,
	      const SequencerPosition &spos);
  int _rmattrs(coll_t cid, const ghobject_t& oid,
	       const SequencerPosition &spos);

  int collection_getattr(coll_t c, const char *name, void *value, size_t size);
  int collection_getattr(coll_t c, const char *name, bufferlist& bl);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);

  int _collection_setattr(coll_t c, const char *name, const void *value, size_t size);
  int _collection_rmattr(coll_t c, const char *name);
  int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset);
  int _collection_remove_recursive(const coll_t &cid,
				   const SequencerPosition &spos);
  int _collection_rename(const coll_t &cid, const coll_t &ncid,
			 const SequencerPosition& spos);

  // collections
  int list_collections(vector<coll_t>& ls);
  int collection_version_current(coll_t c, uint32_t *version);
  int collection_stat(coll_t c, struct stat *st);
  bool collection_exists(coll_t c);
  bool collection_empty(coll_t c);
  int collection_list(coll_t c, vector<ghobject_t>& oid);
  int collection_list_partial(coll_t c, ghobject_t start,
			      int min, int max, snapid_t snap,
			      vector<ghobject_t> *ls, ghobject_t *next);
  int collection_list_range(coll_t c, ghobject_t start, ghobject_t end,
                            snapid_t seq, vector<ghobject_t> *ls);

  // omap (see ObjectStore.h for documentation)
  int omap_get(coll_t c, const ghobject_t &oid, bufferlist *header,
	       map<string, bufferlist> *out);
  int omap_get_header(
    coll_t c,
    const ghobject_t &oid,
    bufferlist *out,
    bool allow_eio = false);
  int omap_get_keys(coll_t c, const ghobject_t &oid, set<string> *keys);
  int omap_get_values(coll_t c, const ghobject_t &oid, const set<string> &keys,
		      map<string, bufferlist> *out);
  int omap_check_keys(coll_t c, const ghobject_t &oid, const set<string> &keys,
		      set<string> *out);
  ObjectMap::ObjectMapIterator get_omap_iterator(coll_t c, const ghobject_t &oid);

  int _create_collection(coll_t c);
  int _create_collection(coll_t c, const SequencerPosition &spos) {
    return _create_collection(c);
  }
  int _destroy_collection(coll_t c);
  int _collection_add(coll_t c, coll_t ocid, const ghobject_t& oid);
  int _collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
			      coll_t c, const ghobject_t& o,
			      const SequencerPosition& spos);
  void dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq, OpSequencer *osr);

private:
  void _inject_failure() {}

  // omap
  int _omap_clear(coll_t cid, const ghobject_t &oid,
		  const SequencerPosition &spos);
  int _omap_setkeys(coll_t cid, const ghobject_t &oid,
		    const map<string, bufferlist> &aset,
		    const SequencerPosition &spos);
  int _omap_rmkeys(coll_t cid, const ghobject_t &oid, const set<string> &keys,
		   const SequencerPosition &spos);
  int _omap_rmkeyrange(coll_t cid, const ghobject_t &oid,
		       const string& first, const string& last,
		       const SequencerPosition &spos);
  int _omap_setheader(coll_t cid, const ghobject_t &oid, const bufferlist &bl,
		      const SequencerPosition &spos);
  int _split_collection(coll_t cid, uint32_t bits, uint32_t rem, coll_t dest,
                        const SequencerPosition &spos);
  int _split_collection_create(coll_t cid, uint32_t bits, uint32_t rem,
			       coll_t dest, const SequencerPosition &spos){return 0;}

  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed);

  std::string m_osd_rollback_to_cluster_snap;
  bool m_osd_use_stale_snap;
  bool m_filestore_fail_eio;
  bool m_filestore_do_dump;

  int do_update;

  int _create_current();

  static const string OBJECT;
  static const string COLLECTION;
  static const string COLLECTION_ATTR;
  static const uint32_t COLLECTION_VERSION = 1;
  string collection_attr_prefix(coll_t c);

  int get(const string& prefix, const string& key, bufferlist *out);
  int get_object(const string& key, bufferlist *out) {
    return get(OBJECT, key, out);
  }
  int setkey(const string& prefix, const string& key, bufferlist& out);
  int set_object(const string& key, bufferlist& out) {
      return setkey(OBJECT, key, out);
  }
  int rmkey(const string& prefix, const string& key);
  int rm_object(const string& key) {
      return rmkey(OBJECT, key);
  }

  class SubmitManager {
      Mutex lock;
      uint64_t op_seq;
      uint64_t op_submitted;
      public:
      SubmitManager() :
          lock("JOS::SubmitManager::lock", false, true, false, g_ceph_context),
          op_seq(0), op_submitted(0)
      {}
      uint64_t op_submit_start();
      void op_submit_finish(uint64_t op);
      void set_op_seq(uint64_t seq) {
          Mutex::Locker l(lock);
          op_submitted = op_seq = seq;
      }
      uint64_t get_op_seq() {
          return op_seq;
      }
  } submit_manager;
};

#endif
