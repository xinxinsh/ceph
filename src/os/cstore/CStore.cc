// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "include/int_types.h"

#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <errno.h>
#include <dirent.h>
#include <sys/ioctl.h>

#if defined(__linux__)
#include <linux/fs.h>
#endif

#include <iostream>
#include <map>

#include "include/compat.h"
#include "include/linux_fiemap.h"
#include "compressor/Compressor.h"

#include "common/xattr.h"
#include "cstore_chain_xattr.h"

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN


#include <fstream>
#include <sstream>

#include "CStore.h"
#include "GenericCStoreBackend.h"
#include "XfsCStoreBackend.h"
#include "common/BackTrace.h"
#include "include/types.h"
#include "CStoreFileJournal.h"
#include "cstore_kv.h"

#include "osd/osd_types.h"
#include "include/color.h"
#include "include/buffer.h"

#include "common/Timer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/perf_counters.h"
#include "common/sync_filesystem.h"
#include "common/fd.h"
#include "CStoreHashIndex.h"
#include "DBCStoreObjectMap.h"
#include "kv/KeyValueDB.h"

#include "common/ceph_crypto.h"
using ceph::crypto::SHA1;

#include "include/assert.h"

#include "common/config.h"
#include "common/blkdev.h"

#define tracepoint(...)

#define dout_subsys ceph_subsys_cstore
#undef dout_prefix
#define dout_prefix *_dout << "cstore(" << basedir << ") "

#define COMMIT_SNAP_ITEM "snap_%llu"
#define CLUSTER_SNAP_ITEM "clustersnap_%s"

#define REPLAY_GUARD_XATTR "user.cephos.seq"
#define GLOBAL_REPLAY_GUARD_XATTR "user.cephos.gseq"

// XATTR_SPILL_OUT_NAME as a xattr is used to maintain that indicates whether
// xattrs spill over into DBCStoreObjectMap, if XATTR_SPILL_OUT_NAME exists in file
// xattrs and the value is "no", it indicates no xattrs in DBCStoreObjectMap
#define XATTR_SPILL_OUT_NAME "user.cephos.spill_out"
#define XATTR_NO_SPILL_OUT "0"
#define XATTR_SPILL_OUT "1"
#define OBJ_DATA "odata"
#define PREFIX_COMPRESS "CO"
#define PREFIX_DECOMPRESS "DO"
#define NS_COMPRESS ".ceph-compress"
#define NS_DECOMPRESS ".ceph-decompress"

#define PREFIX_HITSET "HITSET"

//Initial features in new superblock.
static CompatSet get_fs_initial_compat_set() {
  CompatSet::FeatureSet ceph_osd_feature_compat;
  CompatSet::FeatureSet ceph_osd_feature_ro_compat;
  CompatSet::FeatureSet ceph_osd_feature_incompat;
  return CompatSet(ceph_osd_feature_compat, ceph_osd_feature_ro_compat,
		   ceph_osd_feature_incompat);
}

//Features are added here that this CStore supports.
static CompatSet get_fs_supported_compat_set() {
  CompatSet compat =  get_fs_initial_compat_set();
  //Any features here can be set in code, but not in initial superblock
  compat.incompat.insert(CEPH_FS_FEATURE_INCOMPAT_SHARDS);
  return compat;
}

/*
 * object name key structure
 *
 * 2 chars: shard (-- for none, or hex digit, so that we sort properly)
 * encoded u64: poolid + 2^63 (so that it sorts properly)
 * encoded u32: hash (bit reversed)
 *
 * 1 char: '.'
 *
 * escaped string: namespace
 *
 * 1 char: '<', '=', or '>'.  if =, then object key == object name, and
 *         we are followed just by the key.  otherwise, we are followed by
 *         the key and then the object name.
 * escaped string: key
 * escaped string: object name (unless '=' above)
 *
 * encoded u64: snap
 * encoded u64: generation
 */

/*
 * string encoding in the key
 *
 * The key string needs to lexicographically sort the same way that
 * ghobject_t does.  We do this by escaping anything <= to '#' with #
 * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
 * hex digits.
 *
 * We use ! as a terminator for strings; this works because it is < #
 * and will get escaped if it is present in the string.
 *
 */

static void append_escaped(const string &in, string *out) {
  char hexbyte[8];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i <= '#') {
      snprintf(hexbyte, sizeof(hexbyte), "#%02x", (unsigned)*i);
      out->append(hexbyte);
    } else if (*i >= '~') {
      snprintf(hexbyte, sizeof(hexbyte), "~%02x", (unsigned)*i);
      out->append(hexbyte);
    } else {
      out->push_back(*i);
    }
  }
  out->push_back('!');
}

static int decode_escaped(const char *p, string *out) {
  const char *orig_p = p;
  while (*p && *p != '!') {
    if (*p == '#' || *p == '~') {
      unsigned hex;
      int r = sscanf(++p, "%2x", &hex);
      if (r < 1)
	return -EINVAL;
      out->push_back((char)hex);
      p += 2;
    } else {
      out->push_back(*p++);
    }
  }
  return p - orig_p;
}

static void _key_encode_shard(shard_id_t shard, string *key)
{
  // make field ordering match with ghobject_t compare operations
  if (shard == shard_id_t::NO_SHARD) {
    // otherwise ff will sort *after* 0, not before.
    key->append("--");
  } else {
    char buf[32];
    snprintf(buf, sizeof(buf), "%02x", (int)shard);
    key->append(buf);
  }
}
static const char *_key_decode_shard(const char *key, shard_id_t *pshard)
{
  if (key[0] == '-') {
    *pshard = shard_id_t::NO_SHARD;
  } else {
    unsigned shard;
    int r = sscanf(key, "%x", &shard);
    if (r < 1)
      return NULL;
    *pshard = shard_id_t(shard);
  }
  return key + 2;
}

void get_object_key(const ghobject_t& oid, string* key) {
  key->clear();

  _key_encode_shard(oid.shard_id, key);
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);
  key->append(".");

  append_escaped(oid.hobj.nspace, key);

  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    // (ASCII chars < = and > sort in that order, yay)
    if (oid.hobj.get_key() < oid.hobj.oid.name) {
      key->append("<");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else if (oid.hobj.get_key() > oid.hobj.oid.name) {
      key->append(">");
      append_escaped(oid.hobj.get_key(), key);
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
      append_escaped(oid.hobj.oid.name, key);
    }
  } else {
    // no key
    key->append("=");
    append_escaped(oid.hobj.oid.name, key);
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);
}

int get_key_object(string& key, ghobject_t* oid) {
  int r;
  const char *p = key.c_str();

  if (key.length() < 2 + 8 + 4)
    return -2;
  p = _key_decode_shard(p, &oid->shard_id);

  uint64_t pool;
  p = _key_decode_u64(p, &pool);
  oid->hobj.pool = pool - 0x8000000000000000ull;

  unsigned hash;
  p = _key_decode_u32(p, &hash);

  oid->hobj.set_bitwise_key_u32(hash);
  if (*p != '.')
    return -5;
  ++p;

  r = decode_escaped(p, &oid->hobj.nspace);
  if (r < 0)
    return -6;
  p += r + 1;

  if (*p == '=') {
    // no key
    ++p;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -7;
    p += r + 1;
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p;
    string okey;
    r = decode_escaped(p, &okey);
    if (r < 0)
      return -8;
    p += r + 1;
    r = decode_escaped(p, &oid->hobj.oid.name);
    if (r < 0)
      return -9;
    p += r + 1;
    oid->hobj.set_key(okey);
  } else {
    // malformed
    return -10;
  }

  p = _key_decode_u64(p, &oid->hobj.snap.val);
  p = _key_decode_u64(p, &oid->generation);
  if (*p) {
    // if we get something other than a null terminator here, 
    // something goes wrong.
    return -12;
  }  

  return 0;
}

bool CStore::is_object_op(Transaction::Op *op) {
	switch(op->op) {
		case Transaction::OP_TOUCH:
		case Transaction::OP_WRITE:
		case Transaction::OP_ZERO:
		case Transaction::OP_TRUNCATE:
		case Transaction::OP_REMOVE:
		case Transaction::OP_SETATTR:
		case Transaction::OP_SETATTRS:
		case Transaction::OP_RMATTR:
		case Transaction::OP_RMATTRS:
		case Transaction::OP_CLONE:
		case Transaction::OP_CLONERANGE:
		case Transaction::OP_CLONERANGE2:
		case Transaction::OP_COLL_ADD:
		case Transaction::OP_COLL_MOVE:
		case Transaction::OP_COLL_MOVE_RENAME:
		case Transaction::OP_TRY_RENAME:
		case Transaction::OP_OMAP_CLEAR:
		case Transaction::OP_OMAP_SETKEYS:
		case Transaction::OP_OMAP_RMKEYS:
		case Transaction::OP_OMAP_RMKEYRANGE:
		case Transaction::OP_OMAP_SETHEADER:
		  return true;
	}
	return false;
}

// create hit set
void CStore::hit_set_create() {
  utime_t now = ceph_clock_now(NULL);
	HitSet::Params params;

	if ("bloom" == g_conf->cstore_hit_set_type) {
		BloomHitSet::Params *bsp = new BloomHitSet::Params;
		bsp->set_fpp(g_conf->cstore_hit_set_bloom_fpp);
		params = HitSet::Params(bsp);
	} else {
		assert(0 == "unknow hit set type");
	}

  dout(20) << __func__ << " " << params << dendl;
  if (params.get_type() == HitSet::TYPE_BLOOM) {
    BloomHitSet::Params *p =
      static_cast<BloomHitSet::Params*>(params.impl.get());

    // convert false positive rate so it holds up across the full period
    p->set_fpp(p->get_fpp() / g_conf->cstore_hit_set_count);
    if (p->get_fpp() <= 0.0)
      p->set_fpp(.01);  // fpp cannot be zero!

    // if we don't have specified size, estimate target size based on the
    // previous bin!
    if (p->target_size == 0 && hit_set) {
      utime_t dur = now - hit_set_start_stamp;
      unsigned unique = hit_set->approx_unique_insert_count();
      dout(20) << __func__ << " previous set had approx " << unique
	       << " unique items over " << dur << " seconds" << dendl;
      p->target_size = (double)unique * (double)g_conf->cstore_hit_set_period
		     / (double)dur;
    }
    if (p->target_size < static_cast<uint64_t>(g_conf->cstore_hit_set_min_size))
      p->target_size = g_conf->cstore_hit_set_min_size;

    if (p->target_size > static_cast<uint64_t>(g_conf->cstore_hit_set_max_size))
      p->target_size = g_conf->cstore_hit_set_max_size;

    p->seed = now.sec();

    dout(10) << __func__ << " target_size " << p->target_size
	     << " fpp " << p->get_fpp() << dendl;
  }
  hit_set.reset(new HitSet(params));
  hit_set_start_stamp = now;
}

void CStore::hit_set_clear() {
	dout(20) << __func__ << dendl;
	Mutex::Locker l(hit_set_lock);
	hit_set.reset();
	hit_set_start_stamp = utime_t();
	hit_set_map.clear();
}

ghobject_t CStore::get_hit_set_archive_object(utime_t start) {
	ostringstream ss;
	ss << "hit_set_archive_";
	start.gmtime(ss);
  ghobject_t hoid(hobject_t(sobject_t(ss.str(), CEPH_NOSNAP), "",
			 		 0, -1, g_conf->cstore_hit_set_namespace));
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid; 
}

// setup hit set when mount
void CStore::hit_set_setup() {
	dout(20) << __func__ << dendl;
	bufferlist bl;
	bufferlist::iterator p;
  DBCStoreObjectMap::CStoreObjectMapIterator it;
	hit_set_t hitset;

	// load hit set
  it = object_map->get_whole_iterator(PREFIX_HITSET);
  for(it->lower_bound(string()); it->valid(); it->next()) {
		dout(20) << __func__ << " value " << it->value().length() << dendl;
    bl = it->value();
    p = bl.begin();
    try {
      ::decode(hitset, p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode hit set " << dendl;
    }
		dout(20) << __func__ << " start " << hitset.start.to_msec() << dendl;
		hit_set_map.insert(make_pair(hitset.start.to_msec(), hitset.hitset));
  }

  // create a hit set 
	hit_set_create();
}

// persistent hit set
void CStore::hit_set_persist() {
	string key;
	map<string, bufferlist> vals;

  utime_t now = ceph_clock_now(NULL);
	hit_set_t hit(hit_set_start_stamp, now, *hit_set);
	ghobject_t oid = get_hit_set_archive_object(hit.start);

	hit_set->seal();
	get_object_key(oid, &key);
	::encode(hit, vals[key]);

	dout(20) << __func__ << " archive " << oid << " start " << hit.start.to_msec() << " bl len " << vals[key].length() << dendl;

	object_map->set_keys_by_prefix(PREFIX_HITSET, vals);

	hit_set_map.insert(make_pair(hit_set_start_stamp.to_msec(), *hit_set));

	uint32_t size = g_conf->cstore_hit_set_count > 0 ? g_conf->cstore_hit_set_count - 1 : 0;

	hit_set_trim(size);

	// create new hit set
	hit_set_create();
}

// check if timeout is elapsed
bool CStore::hit_set_timeout() {
  utime_t now = ceph_clock_now(NULL);
	bool timeout = (hit_set_start_stamp + g_conf->cstore_hit_set_period < now);
	dout(30) << __func__ << " timeout " << timeout << " start " << hit_set_start_stamp << " period " << g_conf->cstore_hit_set_period << " now " << now << dendl;
	return timeout;
}

// trim hit set
void CStore::hit_set_trim(uint32_t max) {
  uint32_t count = hit_set_map.size();
	dout(20) << __func__ << " count " << count << " max " << max << dendl;
	set<string> keys;

	for(map<uint64_t, HitSet>::iterator it = hit_set_map.begin();
			count > max; --count, ++it) {
		string key;
		ghobject_t oid = get_hit_set_archive_object(utime_t(it->first, 0));
		get_object_key(oid, &key);
		keys.insert(key);
	}

	while(hit_set_map.size() > max)
		if (!hit_set_map.empty()) {
			map<uint64_t, HitSet>::iterator it = hit_set_map.begin();
			hit_set_map.erase(it);
		}

	object_map->rm_keys_by_prefix(PREFIX_HITSET, keys);
	object_map->sync();
}

int CStore::validate_hobject_key(const hobject_t &obj) const
{
  unsigned len = CStoreLFNIndex::get_max_escaped_name_len(obj);
  return len > m_filestore_max_xattr_value_size ? -ENAMETOOLONG : 0;
}

int CStore::get_block_device_fsid(const string& path, uuid_d *fsid)
{
  // make sure we don't try to use aio or direct_io (and get annoying
  // error messages from failing to do so); performance implications
  // should be irrelevant for this use
  CStoreFileJournal j(*fsid, 0, 0, path.c_str(), false, false);
  return j.peek_fsid(*fsid);
}

void CStore::FSPerfTracker::update_from_perfcounters(
  PerfCounters &logger)
{
  os_commit_latency.consume_next(
    logger.get_tavg_ms(
      l_os_j_lat));
  os_apply_latency.consume_next(
    logger.get_tavg_ms(
      l_os_apply_lat));
}


ostream& operator<<(ostream& out, const CStore::OpSequencer& s)
{
  assert(&out);
  return out << *s.parent;
}

int CStore::get_cdir(const coll_t& cid, char *s, int len)
{
  const string &cid_str(cid.to_str());
  return snprintf(s, len, "%s/current/%s", basedir.c_str(), cid_str.c_str());
}

int CStore::get_index(const coll_t& cid, CStoreIndex *index)
{
  int r = index_manager.get_index(cid, basedir, index);
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int CStore::init_index(const coll_t& cid)
{
  char path[PATH_MAX];
  get_cdir(cid, path, sizeof(path));
  int r = index_manager.init_index(cid, path, target_version);
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int CStore::lfn_find(const ghobject_t& oid, const CStoreIndex& index, CIndexedPath *path)
{
  CIndexedPath path2;
  if (!path)
    path = &path2;
  int r, exist;
  assert(NULL != index.index);
  r = (index.index)->lookup(oid, path, &exist);
  if (r < 0) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  if (!exist)
    return -ENOENT;
  return 0;
}

int CStore::lfn_truncate(const coll_t& cid, const ghobject_t& oid, off_t length)
{
  CFDRef fd;
  int r = lfn_open(cid, oid, false, &fd);
  if (r < 0)
    return r;
  r = ::ftruncate(**fd, length);
  if (r < 0)
    r = -errno;
  if (r >= 0 && m_filestore_sloppy_crc) {
    int rc = backend->_crc_update_truncate(**fd, length);
    assert(rc >= 0);
  }
  lfn_close(fd);
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int CStore::lfn_stat(const coll_t& cid, const ghobject_t& oid, struct stat *buf)
{
  CIndexedPath path;
  CStoreIndex index;
  int r = get_index(cid, &index);
  if (r < 0)
    return r;

  assert(NULL != index.index);
  RWLock::RLocker l((index.index)->access_lock);

  r = lfn_find(oid, index, &path);
  if (r < 0)
    return r;
  r = ::stat(path->path(), buf);
  if (r < 0)
    r = -errno;
  return r;
}

int CStore::lfn_open(const coll_t& cid,
			const ghobject_t& oid,
			bool create,
			CFDRef *outfd,
                        CStoreIndex *index)
{
  assert(outfd);
  int r = 0;
  bool need_lock = true;
  int flags = O_RDWR;

  if (create)
    flags |= O_CREAT;
  if (g_conf->filestore_odsync_write) {
    flags |= O_DSYNC;
  }

  CStoreIndex index2;
  if (!index) {
    index = &index2;
  }
  if (!((*index).index)) {
    r = get_index(cid, index);
    if (r < 0) {
      dout(10) << __func__ << " could not get index r = " << r << dendl;
      return r;
    }
  } else {
    need_lock = false;
  }

  int fd, exist;
  assert(NULL != (*index).index);
  if (need_lock) {
    ((*index).index)->access_lock.get_write();
  }
  if (!replaying) {
    *outfd = fdcache.lookup(oid);
    if (*outfd) {
      if (need_lock) {
        ((*index).index)->access_lock.put_write();
      }
      return 0;
    }
  }


  CIndexedPath path2;
  CIndexedPath *path = &path2;

  r = (*index)->lookup(oid, path, &exist);
  if (r < 0) {
    derr << "could not find " << oid << " in index: "
      << cpp_strerror(-r) << dendl;
    goto fail;
  }

  r = ::open((*path)->path(), flags, 0644);
  if (r < 0) {
    r = -errno;
    dout(10) << "error opening file " << (*path)->path() << " with flags="
      << flags << ": " << cpp_strerror(-r) << dendl;
    goto fail;
  }
  fd = r;
  if (create && (!exist)) {
    r = (*index)->created(oid, (*path)->path());
    if (r < 0) {
      VOID_TEMP_FAILURE_RETRY(::close(fd));
      derr << "error creating " << oid << " (" << (*path)->path()
          << ") in index: " << cpp_strerror(-r) << dendl;
      goto fail;
    }
    r = chain_fsetxattr<true, true>(
      fd, XATTR_SPILL_OUT_NAME,
      XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT));
    if (r < 0) {
      VOID_TEMP_FAILURE_RETRY(::close(fd));
      derr << "error setting spillout xattr for oid " << oid << " (" << (*path)->path()
                     << "):" << cpp_strerror(-r) << dendl;
      goto fail;
    }
  }

  if (!replaying) {
    bool existed;
    *outfd = fdcache.add(oid, fd, &existed);
    if (existed) {
      TEMP_FAILURE_RETRY(::close(fd));
    }
  } else {
    *outfd = CFDRef(new CStoreFDCache::FD(fd));
  }

  if (need_lock) {
    ((*index).index)->access_lock.put_write();
  }

  return 0;

 fail:

  if (need_lock) {
    ((*index).index)->access_lock.put_write();
  }

  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

void CStore::lfn_close(CFDRef fd)
{
}

int CStore::lfn_link(const coll_t& c, const coll_t& newcid, const ghobject_t& o, const ghobject_t& newoid)
{
  dout(25) << __func__ << " " << newcid << "/" << newoid  << " to " << c << "/" << o << dendl;
  CStoreIndex index_new, index_old;
  CIndexedPath path_new, path_old;
  int exist;
  int r;
  bool index_same = false;
  if (c < newcid) {
    r = get_index(newcid, &index_new);
    if (r < 0)
      return r;
    r = get_index(c, &index_old);
    if (r < 0)
      return r;
  } else if (c == newcid) {
    r = get_index(c, &index_old);
    if (r < 0)
      return r;
    index_new = index_old;
    index_same = true;
  } else {
    r = get_index(c, &index_old);
    if (r < 0)
      return r;
    r = get_index(newcid, &index_new);
    if (r < 0)
      return r;
  }

  assert(NULL != index_old.index);
  assert(NULL != index_new.index);

  if (!index_same) {

    RWLock::RLocker l1((index_old.index)->access_lock);

    r = index_old->lookup(o, &path_old, &exist);
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }
    if (!exist)
      return -ENOENT;

    RWLock::WLocker l2((index_new.index)->access_lock);

    r = index_new->lookup(newoid, &path_new, &exist);
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }
    if (exist)
      return -EEXIST;

    dout(25) << "lfn_link path_old: " << path_old << dendl;
    dout(25) << "lfn_link path_new: " << path_new << dendl;
    r = ::link(path_old->path(), path_new->path());
    if (r < 0)
      return -errno;

    r = index_new->created(newoid, path_new->path());
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }
  } else {
    RWLock::WLocker l1((index_old.index)->access_lock);

    r = index_old->lookup(o, &path_old, &exist);
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }
    if (!exist)
      return -ENOENT;

    r = index_new->lookup(newoid, &path_new, &exist);
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }
    if (exist)
      return -EEXIST;

    dout(25) << "lfn_link path_old: " << path_old << dendl;
    dout(25) << "lfn_link path_new: " << path_new << dendl;
    r = ::link(path_old->path(), path_new->path());
    if (r < 0)
      return -errno;

    // make sure old fd for unlinked/overwritten file is gone
    fdcache.clear(newoid);

    r = index_new->created(newoid, path_new->path());
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }
  }
  return 0;
}

int CStore::lfn_unlink(const coll_t& cid, const ghobject_t& o,
			  const CStoreSequencerPosition &spos,
			  bool force_clear_omap)
{
  dout(25) << __func__ << " " << cid << "/" << o << dendl;
  CStoreIndex index;
  int r = get_index(cid, &index);
  if (r < 0) {
    dout(25) << __func__ << " get_index failed " << cpp_strerror(r) << dendl;
    return r;
  }

  assert(NULL != index.index);
  RWLock::WLocker l((index.index)->access_lock);

  {
    CIndexedPath path;
    int hardlink;
    r = index->lookup(o, &path, &hardlink);
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }

    if (!force_clear_omap) {
      if (hardlink == 0) {
          if (!m_disable_wbthrottle) {
	    wbthrottle.clear_object(o); // should be only non-cache ref
          }
	  fdcache.clear(o);
	  return 0;
      } else if (hardlink == 1) {
	  force_clear_omap = true;
      }
    }
    if (force_clear_omap) {
      dout(20) << __func__ << ": clearing omap on " << o
	       << " in cid " << cid << dendl;
      r = object_map->clear(o, &spos);
      if (r < 0 && r != -ENOENT) {
	dout(25) << __func__ << " omap clear failed " << cpp_strerror(r) << dendl;
	assert(!m_filestore_fail_eio || r != -EIO);
	return r;
      }
      if (g_conf->filestore_debug_inject_read_err) {
	debug_obj_on_delete(o);
      }
      if (!m_disable_wbthrottle) {
        wbthrottle.clear_object(o); // should be only non-cache ref
      }
      fdcache.clear(o);
    } else {
      /* Ensure that replay of this op doesn't result in the object_map
       * going away.
       */
      if (!backend->can_checkpoint())
	object_map->sync(&o, &spos);
    }
  }
  r = index->unlink(o);
  if (r < 0) {
    dout(25) << __func__ << " index unlink failed " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

int CStore::lfn_unlink(const coll_t& cid, const ghobject_t& o,
			  bool force_clear_omap)
{
  dout(25) << __func__ << " " << cid << "/" << o << dendl;
  CStoreIndex index;
  int r = get_index(cid, &index);
  if (r < 0) {
    dout(25) << __func__ << " get_index failed " << cpp_strerror(r) << dendl;
    return r;
  }

  assert(NULL != index.index);
  RWLock::WLocker l((index.index)->access_lock);

  {
    CIndexedPath path;
    int hardlink;
    r = index->lookup(o, &path, &hardlink);
    if (r < 0) {
      assert(!m_filestore_fail_eio || r != -EIO);
      return r;
    }

    if (hardlink == 0) {
      if (!m_disable_wbthrottle) {
	wbthrottle.clear_object(o); // should be only non-cache ref
      }
      fdcache.clear(o);
      return 0;
    } else {
      if (g_conf->filestore_debug_inject_read_err) {
	debug_obj_on_delete(o);
      }
      if (!m_disable_wbthrottle) {
	wbthrottle.clear_object(o); // should be only non-cache ref
      }
      fdcache.clear(o);
    }
  }

  r = index->unlink(o);
  if (r < 0 && r != -ENOENT) {
    dout(25) << __func__ << " index unlink failed " << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

CStore::CStore(const std::string &base, const std::string &jdev, osflagbits_t flags, const char *name, bool do_update) :
  CStoreJournalingObjectStore(base),
  internal_name(name),
  basedir(base), journalpath(jdev),
  generic_flags(flags),
  blk_size(0),
  fsid_fd(-1), op_fd(-1),
  basedir_fd(-1), current_fd(-1),
  backend(NULL),
  index_manager(do_update),
  lock("CStore::lock"),
  force_sync(false),
  timer_lock("timer_lock"),
  timer(g_ceph_context, timer_lock),
  stop(false), sync_thread(this),
  fdcache(g_ceph_context),
  wbthrottle(g_ceph_context),
  next_osr_id(0),
  m_disable_wbthrottle(g_conf->filestore_odsync_write || 
                      !g_conf->filestore_wbthrottle_enable),
  throttle_ops(g_conf->filestore_caller_concurrency),
  throttle_bytes(g_conf->filestore_caller_concurrency),
  m_ondisk_finisher_num(g_conf->filestore_ondisk_finisher_threads),
  m_apply_finisher_num(g_conf->filestore_apply_finisher_threads),
  op_tp(g_ceph_context, "CStore::op_tp", "tp_cstore_op", g_conf->filestore_op_threads, "ctore_op_threads"),
  op_wq(this, g_conf->filestore_op_thread_timeout,
	g_conf->filestore_op_thread_suicide_timeout, &op_tp),
  comp_tp(g_ceph_context, "CStore::comp_tp", "tp_cstore_comp", g_conf->cstore_comp_threads, "cstore_comp_threads"),
  comp_wq(this, g_conf->cstore_comp_thread_timeout,
	g_conf->cstore_comp_thread_suicide_timeout, &comp_tp),
  logger(NULL),
  read_error_lock("CStore::read_error_lock"),
  m_filestore_commit_timeout(g_conf->filestore_commit_timeout),
  m_filestore_journal_parallel(g_conf->filestore_journal_parallel ),
  m_filestore_journal_trailing(g_conf->filestore_journal_trailing),
  m_filestore_journal_writeahead(g_conf->filestore_journal_writeahead),
  m_filestore_fiemap_threshold(g_conf->filestore_fiemap_threshold),
  m_filestore_max_sync_interval(g_conf->filestore_max_sync_interval),
  m_filestore_min_sync_interval(g_conf->filestore_min_sync_interval),
  m_filestore_fail_eio(g_conf->filestore_fail_eio),
  m_filestore_fadvise(g_conf->filestore_fadvise),
  do_update(do_update),
  m_journal_dio(g_conf->journal_dio),
  m_journal_aio(g_conf->journal_aio),
  m_journal_force_aio(g_conf->journal_force_aio),
  m_osd_rollback_to_cluster_snap(g_conf->osd_rollback_to_cluster_snap),
  m_osd_use_stale_snap(g_conf->osd_use_stale_snap),
  m_filestore_do_dump(false),
  m_filestore_dump_fmt(true),
  m_filestore_sloppy_crc(g_conf->filestore_sloppy_crc),
  m_filestore_sloppy_crc_block_size(g_conf->filestore_sloppy_crc_block_size),
  m_filestore_max_alloc_hint_size(g_conf->filestore_max_alloc_hint_size),
  m_fs_type(0),
  m_filestore_max_inline_xattr_size(0),
  m_filestore_max_inline_xattrs(0),
  m_filestore_max_xattr_value_size(0),
  m_block_size(g_conf->cstore_block_size),
  m_compress_type(g_conf->cstore_compress_type),
  comp_lock("CStore::comp_lock"),
  op_lock("CStore::op_lock"),
	hit_set_lock("CStore::hit_set_lock")
{
  m_filestore_kill_at.set(g_conf->filestore_kill_at);
  for (int i = 0; i < m_ondisk_finisher_num; ++i) {
    ostringstream oss;
    oss << "cstore-ondisk-" << i;
    Finisher *f = new Finisher(g_ceph_context, oss.str(), "fn_odsk_cstore");
    ondisk_finishers.push_back(f);
  }
  for (int i = 0; i < m_apply_finisher_num; ++i) {
    ostringstream oss;
    oss << "cstore-apply-" << i;
    Finisher *f = new Finisher(g_ceph_context, oss.str(), "fn_appl_cstore");
    apply_finishers.push_back(f);
  }

  ostringstream oss;
  oss << basedir << "/current";
  current_fn = oss.str();

  ostringstream sss;
  sss << basedir << "/current/commit_op_seq";
  current_op_seq_fn = sss.str();

  ostringstream omss;
  omss << basedir << "/current/omap";
  omap_dir = omss.str();

  // initialize logger
  PerfCountersBuilder plb(g_ceph_context, internal_name, l_os_first, l_os_last);

  plb.add_u64(l_os_jq_ops, "journal_queue_ops", "Operations in journal queue");
  plb.add_u64_counter(l_os_j_ops, "journal_ops", "Total journal entries written");
  plb.add_u64(l_os_jq_bytes, "journal_queue_bytes", "Size of journal queue");
  plb.add_u64_counter(l_os_j_bytes, "journal_bytes", "Total operations size in journal");
  plb.add_time_avg(l_os_j_wlat, "journal_wait_latency", "Average journal wait latency in queue");
  plb.add_time_avg(l_os_j_lat, "journal_latency", "Average journal queue completing latency");
  plb.add_u64_counter(l_os_j_wr, "journal_wr", "Journal write IOs");
  plb.add_u64_avg(l_os_j_wr_bytes, "journal_wr_bytes", "Journal data written");
  plb.add_u64(l_os_oq_max_ops, "op_queue_max_ops", "Max operations in writing to FS queue");
  plb.add_u64(l_os_oq_ops, "op_queue_ops", "Operations in writing to FS queue");
  plb.add_u64_counter(l_os_ops, "ops", "Operations written to store");
  plb.add_u64(l_os_oq_max_bytes, "op_queue_max_bytes", "Max data in writing to FS queue");
  plb.add_u64(l_os_oq_bytes, "op_queue_bytes", "Size of writing to FS queue");
  plb.add_u64_counter(l_os_bytes, "bytes", "Data written to store");
  plb.add_time_avg(l_os_apply_lat, "apply_latency", "Apply latency");
  plb.add_u64(l_os_committing, "committing", "Is currently committing");

  plb.add_u64_counter(l_os_commit, "commitcycle", "Commit cycles");
  plb.add_time_avg(l_os_commit_len, "commitcycle_interval", "Average interval between commits");
  plb.add_time_avg(l_os_commit_lat, "commitcycle_latency", "Average latency of commit");
  plb.add_u64_counter(l_os_j_full, "journal_full", "Journal writes while full");
  plb.add_time_avg(l_os_queue_lat, "queue_transaction_latency_avg", "Store operation queue latency");
  plb.add_time_avg(l_os_op_lat, "do_op_latency_avg", "do op latency");
  plb.add_time_avg(l_os_write_lat, "write_latency_avg", "write latency");
  plb.add_time_avg(l_os_setattr_lat, "setattr_latency_avg", "setattr latency");
  plb.add_time_avg(l_os_setattrs_lat, "setattrs_latency_avg", "setattrs latency");
  plb.add_time_avg(l_os_setomap_lat, "setomap_latency_avg", "set omap latency");

  logger = plb.create_perf_counters();

  g_ceph_context->get_perfcounters_collection()->add(logger);
  g_ceph_context->_conf->add_observer(this);

  superblock.compat_features = get_fs_initial_compat_set();
}

CStore::~CStore()
{
  for (vector<Finisher*>::iterator it = ondisk_finishers.begin(); it != ondisk_finishers.end(); ++it) {
    delete *it;
    *it = NULL;
  }
  for (vector<Finisher*>::iterator it = apply_finishers.begin(); it != apply_finishers.end(); ++it) {
    delete *it;
    *it = NULL;
  }
  g_ceph_context->_conf->remove_observer(this);
  g_ceph_context->get_perfcounters_collection()->remove(logger);

  if (journal)
    journal->logger = NULL;
  delete logger;

  if (m_filestore_do_dump) {
    dump_stop();
  }
}

static void get_attrname(const char *name, char *buf, int len)
{
  snprintf(buf, len, "user.ceph.%s", name);
}

bool cparse_attrname(char **name)
{
  if (strncmp(*name, "user.ceph.", 10) == 0) {
    *name += 10;
    return true;
  }
  return false;
}

void CStore::collect_metadata(map<string,string> *pm)
{
  char partition_path[PATH_MAX];
  char dev_node[PATH_MAX];
  int rc = 0;

  (*pm)["cstore_backend"] = backend->get_name();
  ostringstream ss;
  ss << "0x" << std::hex << m_fs_type << std::dec;
  (*pm)["cstore_f_type"] = ss.str();

  if (g_conf->filestore_collect_device_partition_information) {
    rc = get_device_by_uuid(get_fsid(), "PARTUUID", partition_path,
          dev_node);
  } else {
    rc = -EINVAL;
  }

  switch (rc) {
    case -EOPNOTSUPP:
    case -EINVAL:
      (*pm)["backend_cstore_partition_path"] = "unknown";
      (*pm)["backend_cstore_dev_node"] = "unknown";
      break;
    case -ENODEV:
      (*pm)["backend_cstore_partition_path"] = string(partition_path);
      (*pm)["backend_cstore_dev_node"] = "unknown";
      break;
    default:
      (*pm)["backend_cstore_partition_path"] = string(partition_path);
      (*pm)["backend_cstore_dev_node"] = string(dev_node);
  }
}

int CStore::statfs(struct statfs *buf)
{
  if (::statfs(basedir.c_str(), buf) < 0) {
    int r = -errno;
    assert(!m_filestore_fail_eio || r != -EIO);
    assert(r != -ENOENT);
    return r;
  }
  return 0;
}


void CStore::new_journal()
{
  if (journalpath.length()) {
    dout(10) << "open_journal at " << journalpath << dendl;
    journal = new CStoreFileJournal(fsid, &finisher, &sync_cond, journalpath.c_str(),
			      m_journal_dio, m_journal_aio, m_journal_force_aio);
    if (journal)
      journal->logger = logger;
  }
  return;
}

int CStore::dump_journal(ostream& out)
{
  int r;

  if (!journalpath.length())
    return -EINVAL;

  CStoreFileJournal *journal = new CStoreFileJournal(fsid, &finisher, &sync_cond, journalpath.c_str(), m_journal_dio);
  r = journal->dump(out);
  delete journal;
  return r;
}

CStoreBackend *CStoreBackend::create(long f_type, CStore *fs)
{
  switch (f_type) {
#if defined(__linux__)
# ifdef HAVE_LIBXFS
  case XFS_SUPER_MAGIC:
    return new XfsCStoreBackend(fs);
# endif
#endif
  default:
    return new GenericCStoreBackend(fs);
  }
}

void CStore::create_backend(long f_type)
{
  m_fs_type = f_type;

  assert(backend == NULL);
  backend = CStoreBackend::create(f_type, this);

  dout(0) << "backend " << backend->get_name()
	  << " (magic 0x" << std::hex << f_type << std::dec << ")"
	  << dendl;

  switch (f_type) {
#if defined(__linux__)
  case BTRFS_SUPER_MAGIC:
    if (!m_disable_wbthrottle){
      wbthrottle.set_fs(CStoreWBThrottle::BTRFS);
    }
    break;

  case XFS_SUPER_MAGIC:
    // wbthrottle is constructed with fs(CStoreWBThrottle::XFS)
    break;
#endif
  }

  set_xattr_limits_via_conf();
}

int CStore::mkfs()
{
  int ret = 0;
  char fsid_fn[PATH_MAX];
  uuid_d old_fsid;

  dout(1) << "mkfs in " << basedir << dendl;
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);
  if (basedir_fd < 0) {
    ret = -errno;
    derr << "mkfs failed to open base dir " << basedir << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  // open+lock fsid
  snprintf(fsid_fn, sizeof(fsid_fn), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(fsid_fn, O_RDWR|O_CREAT, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "mkfs: failed to open " << fsid_fn << ": " << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
  }

  if (lock_fsid() < 0) {
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  if (read_fsid(fsid_fd, &old_fsid) < 0 || old_fsid.is_zero()) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << "mkfs generated fsid " << fsid << dendl;
    } else {
      dout(1) << "mkfs using provided fsid " << fsid << dendl;
    }

    char fsid_str[40];
    fsid.print(fsid_str);
    strcat(fsid_str, "\n");
    ret = ::ftruncate(fsid_fd, 0);
    if (ret < 0) {
      ret = -errno;
      derr << "mkfs: failed to truncate fsid: "
	   << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    ret = safe_write(fsid_fd, fsid_str, strlen(fsid_str));
    if (ret < 0) {
      derr << "mkfs: failed to write fsid: "
	   << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    if (::fsync(fsid_fd) < 0) {
      ret = -errno;
      derr << "mkfs: close failed: can't write fsid: "
	   << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    dout(10) << "mkfs fsid is " << fsid << dendl;
  } else {
    if (!fsid.is_zero() && fsid != old_fsid) {
      derr << "mkfs on-disk fsid " << old_fsid << " != provided " << fsid << dendl;
      ret = -EINVAL;
      goto close_fsid_fd;
    }
    fsid = old_fsid;
    dout(1) << "mkfs fsid is already set to " << fsid << dendl;
  }

  // version stamp
  ret = write_version_stamp();
  if (ret < 0) {
    derr << "mkfs: write_version_stamp() failed: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  // superblock
  superblock.omap_backend = g_conf->filestore_omap_backend;
  ret = write_superblock();
  if (ret < 0) {
    derr << "mkfs: write_superblock() failed: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  struct statfs basefs;
  ret = ::fstatfs(basedir_fd, &basefs);
  if (ret < 0) {
    ret = -errno;
    derr << "mkfs cannot fstatfs basedir "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  create_backend(basefs.f_type);

  ret = backend->create_current();
  if (ret < 0) {
    derr << "mkfs: failed to create current/ " << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  // write initial op_seq
  {
    uint64_t initial_seq = 0;
    int fd = read_op_seq(&initial_seq);
    if (fd < 0) {
      ret = fd;
      derr << "mkfs: failed to create " << current_op_seq_fn << ": "
	   << cpp_strerror(ret) << dendl;
      goto close_fsid_fd;
    }
    if (initial_seq == 0) {
      ret = write_op_seq(fd, 1);
      if (ret < 0) {
	VOID_TEMP_FAILURE_RETRY(::close(fd));
	derr << "mkfs: failed to write to " << current_op_seq_fn << ": "
	     << cpp_strerror(ret) << dendl;
	goto close_fsid_fd;
      }

      if (backend->can_checkpoint()) {
	// create snap_1 too
	current_fd = ::open(current_fn.c_str(), O_RDONLY);
	assert(current_fd >= 0);
	char s[NAME_MAX];
	snprintf(s, sizeof(s), COMMIT_SNAP_ITEM, 1ull);
	ret = backend->create_checkpoint(s, NULL);
	VOID_TEMP_FAILURE_RETRY(::close(current_fd));
	if (ret < 0 && ret != -EEXIST) {
	  VOID_TEMP_FAILURE_RETRY(::close(fd));
	  derr << "mkfs: failed to create snap_1: " << cpp_strerror(ret) << dendl;
	  goto close_fsid_fd;
	}
      }
    }
    VOID_TEMP_FAILURE_RETRY(::close(fd));
  }
  ret = KeyValueDB::test_init(superblock.omap_backend, omap_dir);
  if (ret < 0) {
    derr << "mkfs failed to create " << g_conf->filestore_omap_backend << dendl;
    ret = -1;
    goto close_fsid_fd;
  }
  dout(1) << g_conf->filestore_omap_backend << " db exists/created" << dendl;

  // journal?
  ret = mkjournal();
  if (ret)
    goto close_fsid_fd;

  ret = write_meta("type", "cstore");
  if (ret)
    goto close_fsid_fd;

  dout(1) << "mkfs done in " << basedir << dendl;
  ret = 0;

 close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
 close_basedir_fd:
  VOID_TEMP_FAILURE_RETRY(::close(basedir_fd));
  delete backend;
  backend = NULL;
  return ret;
}

int CStore::mkjournal()
{
  // read fsid
  int ret;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
  int fd = ::open(fn, O_RDONLY, 0644);
  if (fd < 0) {
    int err = errno;
    derr << "CStore::mkjournal: open error: " << cpp_strerror(err) << dendl;
    return -err;
  }
  ret = read_fsid(fd, &fsid);
  if (ret < 0) {
    derr << "CStore::mkjournal: read error: " << cpp_strerror(ret) << dendl;
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));

  ret = 0;

  new_journal();
  if (journal) {
    ret = journal->check();
    if (ret < 0) {
      ret = journal->create();
      if (ret)
	derr << "mkjournal error creating journal on " << journalpath
		<< ": " << cpp_strerror(ret) << dendl;
      else
	dout(0) << "mkjournal created journal on " << journalpath << dendl;
    }
    delete journal;
    journal = 0;
  }
  return ret;
}

int CStore::read_fsid(int fd, uuid_d *uuid)
{
  char fsid_str[40];
  memset(fsid_str, 0, sizeof(fsid_str));
  int ret = safe_read(fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;
  if (ret == 8) {
    // old 64-bit fsid... mirror it.
    *(uint64_t*)&uuid->bytes()[0] = *(uint64_t*)fsid_str;
    *(uint64_t*)&uuid->bytes()[8] = *(uint64_t*)fsid_str;
    return 0;
  }

  if (ret > 36)
    fsid_str[36] = 0;
  else
    fsid_str[ret] = 0;
  if (!uuid->parse(fsid_str))
    return -EINVAL;
  return 0;
}

int CStore::lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    int err = errno;
    dout(0) << "lock_fsid failed to lock " << basedir << "/fsid, is another ceph-osd still running? "
	    << cpp_strerror(err) << dendl;
    return -err;
  }
  return 0;
}

bool CStore::test_mount_in_use()
{
  dout(5) << "test_mount basedir " << basedir << " journal " << journalpath << dendl;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());

  // verify fs isn't in use

  fsid_fd = ::open(fn, O_RDWR, 0644);
  if (fsid_fd < 0)
    return 0;   // no fsid, ok.
  bool inuse = lock_fsid() < 0;
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
  return inuse;
}

int CStore::_detect_fs()
{
  struct statfs st;
  int r = ::fstatfs(basedir_fd, &st);
  if (r < 0)
    return -errno;

  blk_size = st.f_bsize;

  create_backend(st.f_type);

  r = backend->detect_features();
  if (r < 0) {
    derr << "_detect_fs: detect_features error: " << cpp_strerror(r) << dendl;
    return r;
  }

  // test xattrs
  char fn[PATH_MAX];
  int x = rand();
  int y = x+1;
  snprintf(fn, sizeof(fn), "%s/xattr_test", basedir.c_str());
  int tmpfd = ::open(fn, O_CREAT|O_WRONLY|O_TRUNC, 0700);
  if (tmpfd < 0) {
    int ret = -errno;
    derr << "_detect_fs unable to create " << fn << ": " << cpp_strerror(ret) << dendl;
    return ret;
  }

  int ret = chain_fsetxattr(tmpfd, "user.test", &x, sizeof(x));
  if (ret >= 0)
    ret = chain_fgetxattr(tmpfd, "user.test", &y, sizeof(y));
  if ((ret < 0) || (x != y)) {
    derr << "Extended attributes don't appear to work. ";
    if (ret)
      *_dout << "Got error " + cpp_strerror(ret) + ". ";
    *_dout << "If you are using ext3 or ext4, be sure to mount the underlying "
	   << "file system with the 'user_xattr' option." << dendl;
    ::unlink(fn);
    VOID_TEMP_FAILURE_RETRY(::close(tmpfd));
    return -ENOTSUP;
  }

  char buf[1000];
  memset(buf, 0, sizeof(buf)); // shut up valgrind
  chain_fsetxattr(tmpfd, "user.test", &buf, sizeof(buf));
  chain_fsetxattr(tmpfd, "user.test2", &buf, sizeof(buf));
  chain_fsetxattr(tmpfd, "user.test3", &buf, sizeof(buf));
  chain_fsetxattr(tmpfd, "user.test4", &buf, sizeof(buf));
  ret = chain_fsetxattr(tmpfd, "user.test5", &buf, sizeof(buf));
  if (ret == -ENOSPC) {
    dout(0) << "limited size xattrs" << dendl;
  }
  chain_fremovexattr(tmpfd, "user.test");
  chain_fremovexattr(tmpfd, "user.test2");
  chain_fremovexattr(tmpfd, "user.test3");
  chain_fremovexattr(tmpfd, "user.test4");
  chain_fremovexattr(tmpfd, "user.test5");

  ::unlink(fn);
  VOID_TEMP_FAILURE_RETRY(::close(tmpfd));

  return 0;
}

int CStore::_sanity_check_fs()
{
  // sanity check(s)

  if (((int)m_filestore_journal_writeahead +
      (int)m_filestore_journal_parallel +
      (int)m_filestore_journal_trailing) > 1) {
    dout(0) << "mount ERROR: more than one of filestore journal {writeahead,parallel,trailing} enabled" << dendl;
    cerr << TEXT_RED
	 << " ** WARNING: more than one of 'filestore journal {writeahead,parallel,trailing}'\n"
	 << "             is enabled in ceph.conf.  You must choose a single journal mode."
	 << TEXT_NORMAL << std::endl;
    return -EINVAL;
  }

  if (!backend->can_checkpoint()) {
    if (!journal || !m_filestore_journal_writeahead) {
      dout(0) << "mount WARNING: no btrfs, and no journal in writeahead mode; data may be lost" << dendl;
      cerr << TEXT_RED
	   << " ** WARNING: no btrfs AND (no journal OR journal not in writeahead mode)\n"
	   << "             For non-btrfs volumes, a writeahead journal is required to\n"
	   << "             maintain on-disk consistency in the event of a crash.  Your conf\n"
	   << "             should include something like:\n"
	   << "        osd journal = /path/to/journal_device_or_file\n"
	   << "        filestore journal writeahead = true\n"
	   << TEXT_NORMAL;
    }
  }

  if (!journal) {
    dout(0) << "mount WARNING: no journal" << dendl;
    cerr << TEXT_YELLOW
	 << " ** WARNING: No osd journal is configured: write latency may be high.\n"
	 << "             If you will not be using an osd journal, write latency may be\n"
	 << "             relatively high.  It can be reduced somewhat by lowering\n"
	 << "             filestore_max_sync_interval, but lower values mean lower write\n"
	 << "             throughput, especially with spinning disks.\n"
	 << TEXT_NORMAL;
  }

  return 0;
}

int CStore::write_superblock()
{
  bufferlist bl;
  ::encode(superblock, bl);
  return safe_write_file(basedir.c_str(), "superblock",
      bl.c_str(), bl.length());
}

int CStore::read_superblock()
{
  bufferptr bp(PATH_MAX);
  int ret = safe_read_file(basedir.c_str(), "superblock",
      bp.c_str(), bp.length());
  if (ret < 0) {
    if (ret == -ENOENT) {
      // If the file doesn't exist write initial CompatSet
      return write_superblock();
    }
    return ret;
  }

  bufferlist bl;
  bl.push_back(std::move(bp));
  bufferlist::iterator i = bl.begin();
  ::decode(superblock, i);
  return 0;
}

int CStore::update_version_stamp()
{
  return write_version_stamp();
}

int CStore::version_stamp_is_valid(uint32_t *version)
{
  bufferptr bp(PATH_MAX);
  int ret = safe_read_file(basedir.c_str(), "store_version",
      bp.c_str(), bp.length());
  if (ret < 0) {
    if (ret == -ENOENT)
      return 0;
    return ret;
  }
  bufferlist bl;
  bl.push_back(std::move(bp));
  bufferlist::iterator i = bl.begin();
  ::decode(*version, i);
  dout(10) << __func__ << " was " << *version << " vs target "
	   << target_version << dendl;
  if (*version == target_version)
    return 1;
  else
    return 0;
}

int CStore::write_version_stamp()
{
  dout(1) << __func__ << " " << target_version << dendl;
  bufferlist bl;
  ::encode(target_version, bl);

  return safe_write_file(basedir.c_str(), "store_version",
      bl.c_str(), bl.length());
}

int CStore::upgrade()
{
  dout(1) << "upgrade" << dendl;
  uint32_t version;
  int r = version_stamp_is_valid(&version);
  if (r < 0)
    return r;
  if (r == 1)
    return 0;

  if (version < 3) {
    derr << "ObjectStore is old at version " << version << ".  Please upgrade to firefly v0.80.x, convert your store, and then upgrade."  << dendl;
    return -EINVAL;
  }

  // nothing necessary in CStore for v3 -> v4 upgrade; we just need to
  // open up DBCStoreObjectMap with the do_upgrade flag, which we already did.
  update_version_stamp();
  return 0;
}

int CStore::read_op_seq(uint64_t *seq)
{
  int op_fd = ::open(current_op_seq_fn.c_str(), O_CREAT|O_RDWR, 0644);
  if (op_fd < 0) {
    int r = -errno;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  char s[40];
  memset(s, 0, sizeof(s));
  int ret = safe_read(op_fd, s, sizeof(s) - 1);
  if (ret < 0) {
    derr << "error reading " << current_op_seq_fn << ": " << cpp_strerror(ret) << dendl;
    VOID_TEMP_FAILURE_RETRY(::close(op_fd));
    assert(!m_filestore_fail_eio || ret != -EIO);
    return ret;
  }
  *seq = atoll(s);
  return op_fd;
}

int CStore::write_op_seq(int fd, uint64_t seq)
{
  char s[30];
  snprintf(s, sizeof(s), "%" PRId64 "\n", seq);
  int ret = TEMP_FAILURE_RETRY(::pwrite(fd, s, strlen(s), 0));
  if (ret < 0) {
    ret = -errno;
    assert(!m_filestore_fail_eio || ret != -EIO);
  }
  return ret;
}

class CompContext : public Context {
public:
  CompContext(CStore *fs, const coll_t &cid, const ghobject_t &oid)
    : fs(fs), cid(cid), oid(oid) {}

  void finish(int r) {
    assert(fs);
    fs->_filter_comp(cid, oid);
  }

private:
  CStore *fs;
  coll_t cid;
  ghobject_t oid;
};

int CStore::mount()
{
  int ret;
  char buf[PATH_MAX];
  uint64_t initial_op_seq;
  set<string> cluster_snaps;
  CompatSet supported_compat_set = get_fs_supported_compat_set();
  DBCStoreObjectMap::CStoreObjectMapIterator omap_it;

  dout(5) << "basedir " << basedir << " journal " << journalpath << dendl;

  ret = set_throttle_params();
  if (ret != 0)
    goto done;

  // make sure global base dir exists
  if (::access(basedir.c_str(), R_OK | W_OK)) {
    ret = -errno;
    derr << "CStore::mount: unable to access basedir '" << basedir << "': "
	 << cpp_strerror(ret) << dendl;
    goto done;
  }

  // get fsid
  snprintf(buf, sizeof(buf), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(buf, O_RDWR, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "CStore::mount: error opening '" << buf << "': "
	 << cpp_strerror(ret) << dendl;
    goto done;
  }

  ret = read_fsid(fsid_fd, &fsid);
  if (ret < 0) {
    derr << "CStore::mount: error reading fsid_fd: " << cpp_strerror(ret)
	 << dendl;
    goto close_fsid_fd;
  }

  if (lock_fsid() < 0) {
    derr << "CStore::mount: lock_fsid failed" << dendl;
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  dout(10) << "mount fsid is " << fsid << dendl;


  uint32_t version_stamp;
  ret = version_stamp_is_valid(&version_stamp);
  if (ret < 0) {
    derr << "CStore::mount: error in version_stamp_is_valid: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  } else if (ret == 0) {
    if (do_update || (int)version_stamp < g_conf->filestore_update_to) {
      derr << "CStore::mount: stale version stamp detected: "
	   << version_stamp
	   << ". Proceeding, do_update "
	   << "is set, performing disk format upgrade."
	   << dendl;
      do_update = true;
    } else {
      ret = -EINVAL;
      derr << "CStore::mount: stale version stamp " << version_stamp
	   << ". Please run the CStore update script before starting the "
	   << "OSD, or set filestore_update_to to " << target_version
	   << " (currently " << g_conf->filestore_update_to << ")"
	   << dendl;
      goto close_fsid_fd;
    }
  }

  ret = read_superblock();
  if (ret < 0) {
    goto close_fsid_fd;
  }

  // Check if this CStore supports all the necessary features to mount
  if (supported_compat_set.compare(superblock.compat_features) == -1) {
    derr << "CStore::mount: Incompatible features set "
	   << superblock.compat_features << dendl;
    ret = -EINVAL;
    goto close_fsid_fd;
  }

  // open some dir handles
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);
  if (basedir_fd < 0) {
    ret = -errno;
    derr << "CStore::mount: failed to open " << basedir << ": "
	 << cpp_strerror(ret) << dendl;
    basedir_fd = -1;
    goto close_fsid_fd;
  }

  // test for btrfs, xattrs, etc.
  ret = _detect_fs();
  if (ret < 0) {
    derr << "CStore::mount: error in _detect_fs: "
	 << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
  }

  {
    list<string> ls;
    ret = backend->list_checkpoints(ls);
    if (ret < 0) {
      derr << "CStore::mount: error in _list_snaps: "<< cpp_strerror(ret) << dendl;
      goto close_basedir_fd;
    }

    long long unsigned c, prev = 0;
    char clustersnap[NAME_MAX];
    for (list<string>::iterator it = ls.begin(); it != ls.end(); ++it) {
      if (sscanf(it->c_str(), COMMIT_SNAP_ITEM, &c) == 1) {
	assert(c > prev);
	prev = c;
	snaps.push_back(c);
      } else if (sscanf(it->c_str(), CLUSTER_SNAP_ITEM, clustersnap) == 1)
	cluster_snaps.insert(*it);
    }
  }

  if (m_osd_rollback_to_cluster_snap.length() &&
      cluster_snaps.count(m_osd_rollback_to_cluster_snap) == 0) {
    derr << "rollback to cluster snapshot '" << m_osd_rollback_to_cluster_snap << "': not found" << dendl;
    ret = -ENOENT;
    goto close_basedir_fd;
  }

  char nosnapfn[200];
  snprintf(nosnapfn, sizeof(nosnapfn), "%s/nosnap", current_fn.c_str());

  if (backend->can_checkpoint()) {
    if (snaps.empty()) {
      dout(0) << "mount WARNING: no consistent snaps found, store may be in inconsistent state" << dendl;
    } else {
      char s[NAME_MAX];
      uint64_t curr_seq = 0;

      if (m_osd_rollback_to_cluster_snap.length()) {
	derr << TEXT_RED
	     << " ** NOTE: rolling back to cluster snapshot " << m_osd_rollback_to_cluster_snap << " **"
	     << TEXT_NORMAL
	     << dendl;
	assert(cluster_snaps.count(m_osd_rollback_to_cluster_snap));
	snprintf(s, sizeof(s), CLUSTER_SNAP_ITEM, m_osd_rollback_to_cluster_snap.c_str());
      } else {
	{
	  int fd = read_op_seq(&curr_seq);
	  if (fd >= 0) {
	    VOID_TEMP_FAILURE_RETRY(::close(fd));
	  }
	}
	if (curr_seq)
	  dout(10) << " current/ seq was " << curr_seq << dendl;
	else
	  dout(10) << " current/ missing entirely (unusual, but okay)" << dendl;

	uint64_t cp = snaps.back();
	dout(10) << " most recent snap from " << snaps << " is " << cp << dendl;

	// if current/ is marked as non-snapshotted, refuse to roll
	// back (without clear direction) to avoid throwing out new
	// data.
	struct stat st;
	if (::stat(nosnapfn, &st) == 0) {
	  if (!m_osd_use_stale_snap) {
	    derr << "ERROR: " << nosnapfn << " exists, not rolling back to avoid losing new data" << dendl;
	    derr << "Force rollback to old snapshotted version with 'osd use stale snap = true'" << dendl;
	    derr << "config option for --osd-use-stale-snap startup argument." << dendl;
	    ret = -ENOTSUP;
	    goto close_basedir_fd;
	  }
	  derr << "WARNING: user forced start with data sequence mismatch: current was " << curr_seq
	       << ", newest snap is " << cp << dendl;
	  cerr << TEXT_YELLOW
	       << " ** WARNING: forcing the use of stale snapshot data **"
	       << TEXT_NORMAL << std::endl;
	}

        dout(10) << "mount rolling back to consistent snap " << cp << dendl;
	snprintf(s, sizeof(s), COMMIT_SNAP_ITEM, (long long unsigned)cp);
      }

      // drop current?
      ret = backend->rollback_to(s);
      if (ret) {
	derr << "CStore::mount: error rolling back to " << s << ": "
	     << cpp_strerror(ret) << dendl;
	goto close_basedir_fd;
      }
    }
  }
  initial_op_seq = 0;

  current_fd = ::open(current_fn.c_str(), O_RDONLY);
  if (current_fd < 0) {
    ret = -errno;
    derr << "CStore::mount: error opening: " << current_fn << ": " << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
  }

  assert(current_fd >= 0);

  op_fd = read_op_seq(&initial_op_seq);
  if (op_fd < 0) {
    ret = op_fd;
    derr << "CStore::mount: read_op_seq failed" << dendl;
    goto close_current_fd;
  }

  dout(5) << "mount op_seq is " << initial_op_seq << dendl;
  if (initial_op_seq == 0) {
    derr << "mount initial op seq is 0; something is wrong" << dendl;
    ret = -EINVAL;
    goto close_current_fd;
  }

  if (!backend->can_checkpoint()) {
    // mark current/ as non-snapshotted so that we don't rollback away
    // from it.
    int r = ::creat(nosnapfn, 0644);
    if (r < 0) {
      ret = -errno;
      derr << "CStore::mount: failed to create current/nosnap" << dendl;
      goto close_current_fd;
    }
    VOID_TEMP_FAILURE_RETRY(::close(r));
  } else {
    // clear nosnap marker, if present.
    ::unlink(nosnapfn);
  }

  if (!(generic_flags & SKIP_MOUNT_OMAP)) {
    KeyValueDB * omap_store = KeyValueDB::create(g_ceph_context,
						 superblock.omap_backend,
						 omap_dir);
    if (omap_store == NULL)
    {
      derr << "Error creating " << superblock.omap_backend << dendl;
      ret = -1;
      goto close_current_fd;
    }

    if (superblock.omap_backend == "rocksdb")
      omap_store->init(g_conf->filestore_rocksdb_options);
    else
      omap_store->init();

    stringstream err;
    if (omap_store->create_and_open(err)) {
      delete omap_store;
      derr << "Error initializing " << superblock.omap_backend
	   << " : " << err.str() << dendl;
      ret = -1;
      goto close_current_fd;
    }

    DBCStoreObjectMap *dbomap = new DBCStoreObjectMap(omap_store);
    ret = dbomap->init(do_update);
    if (ret < 0) {
      delete dbomap;
      derr << "Error initializing DBCStoreObjectMap: " << ret << dendl;
      goto close_current_fd;
    }
    stringstream err2;

    if (g_conf->filestore_debug_omap_check && !dbomap->check(err2)) {
      derr << err2.str() << dendl;
      delete dbomap;
      ret = -EINVAL;
      goto close_current_fd;
    }
    object_map.reset(dbomap);
  }

  // replay compress/decompress wal
  ret = _wal_replay();
  if (ret < 0) {
    derr << "CStore::mount _wal_replay failed with error " << ret << dendl;
    goto close_current_fd;
  }

	// load history hit set
	{
		Mutex::Locker l(hit_set_lock);
	  hit_set_setup();
	}

  // journal
  new_journal();

  // select journal mode?
  if (journal) {
    if (!m_filestore_journal_writeahead &&
	!m_filestore_journal_parallel &&
	!m_filestore_journal_trailing) {
      if (!backend->can_checkpoint()) {
	m_filestore_journal_writeahead = true;
	dout(0) << "mount: enabling WRITEAHEAD journal mode: checkpoint is not enabled" << dendl;
      } else {
	m_filestore_journal_parallel = true;
	dout(0) << "mount: enabling PARALLEL journal mode: fs, checkpoint is enabled" << dendl;
      }
    } else {
      if (m_filestore_journal_writeahead)
	dout(0) << "mount: WRITEAHEAD journal mode explicitly enabled in conf" << dendl;
      if (m_filestore_journal_parallel)
	dout(0) << "mount: PARALLEL journal mode explicitly enabled in conf" << dendl;
      if (m_filestore_journal_trailing)
	dout(0) << "mount: TRAILING journal mode explicitly enabled in conf" << dendl;
    }
    if (m_filestore_journal_writeahead)
      journal->set_wait_on_full(true);
  } else {
    dout(0) << "mount: no journal" << dendl;
  }

  ret = _sanity_check_fs();
  if (ret) {
    derr << "CStore::mount: _sanity_check_fs failed with error "
	 << ret << dendl;
    goto close_current_fd;
  }

  // Cleanup possibly invalid collections
  {
    vector<coll_t> collections;
    ret = list_collections(collections, true);
    if (ret < 0) {
      derr << "Error " << ret << " while listing collections" << dendl;
      goto close_current_fd;
    }
    for (vector<coll_t>::iterator i = collections.begin();
	 i != collections.end();
	 ++i) {
      CStoreIndex index;
      ret = get_index(*i, &index);
      if (ret < 0) {
	derr << "Unable to mount index " << *i
	     << " with error: " << ret << dendl;
	goto close_current_fd;
      }
      assert(NULL != index.index);
      RWLock::WLocker l((index.index)->access_lock);

      index->cleanup();
    }
  }
  if (!m_disable_wbthrottle) {
    wbthrottle.start();
  } else {
    dout(0) << "mount INFO: WbThrottle is disabled" << dendl;
    if (g_conf->filestore_odsync_write) {
      dout(0) << "mount INFO: O_DSYNC write is enabled" << dendl;
    }
  }
  sync_thread.create("cstore_sync");

  if (!(generic_flags & SKIP_JOURNAL_REPLAY)) {
    ret = journal_replay(initial_op_seq);
    if (ret < 0) {
      derr << "mount failed to open journal " << journalpath << ": " << cpp_strerror(ret) << dendl;
      if (ret == -ENOTTY) {
        derr << "maybe journal is not pointing to a block device and its size "
	     << "wasn't configured?" << dendl;
      }

      goto stop_sync;
    }
  }

  {
    stringstream err2;
    if (g_conf->filestore_debug_omap_check && !object_map->check(err2)) {
      derr << err2.str() << dendl;
      ret = -EINVAL;
      goto stop_sync;
    }
  }

  init_temp_collections();

  journal_start();

  op_tp.start();

  comp_tp.start();

  for (vector<Finisher*>::iterator it = ondisk_finishers.begin(); it != ondisk_finishers.end(); ++it) {
    (*it)->start();
  }
  for (vector<Finisher*>::iterator it = apply_finishers.begin(); it != apply_finishers.end(); ++it) {
    (*it)->start();
  }

  timer.init();

  omap_it = object_map->get_whole_iterator(DBCStoreObjectMap::HOBJECT_TO_COLL);
  for(omap_it->lower_bound(string()); omap_it->valid(); omap_it->next()) {
    bufferlist bl = omap_it->value();
    bufferlist::iterator p = bl.begin();
    map_header_t *h = new map_header_t();
    try {
      h->decode(p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode header " << dendl;
			umount();
			return -EIO;
    }

    dout(20) << __func__ << " init periodical event " << dendl;
    CompContext *c = new CompContext(this, h->cid, h->oid);
    {
      Mutex::Locker l(timer_lock);
      timer.add_event_after(g_conf->cstore_compress_interval, c);
    }
  }
  // upgrade?
  if (g_conf->filestore_update_to >= (int)get_target_version()) {
    int err = upgrade();
    if (err < 0) {
      derr << "error converting store" << dendl;
      umount();
      return err;
    }
  }

  // all okay.
  return 0;

stop_sync:
  // stop sync thread
  lock.Lock();
  stop = true;
  sync_cond.Signal();
  lock.Unlock();
  sync_thread.join();
  if (!m_disable_wbthrottle) {
    wbthrottle.stop();
  }
close_current_fd:
  VOID_TEMP_FAILURE_RETRY(::close(current_fd));
  current_fd = -1;
close_basedir_fd:
  VOID_TEMP_FAILURE_RETRY(::close(basedir_fd));
  basedir_fd = -1;
close_fsid_fd:
  VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
done:
  assert(!m_filestore_fail_eio || ret != -EIO);
  return ret;
}

void CStore::init_temp_collections()
{
  dout(10) << __func__ << dendl;
  vector<coll_t> ls;
  int r = list_collections(ls, true);
  assert(r >= 0);

  dout(20) << " ls " << ls << dendl;

  CStoreSequencerPosition spos;

  set<coll_t> temps;
  for (vector<coll_t>::iterator p = ls.begin(); p != ls.end(); ++p)
    if (p->is_temp())
      temps.insert(*p);
  dout(20) << " temps " << temps << dendl;

  for (vector<coll_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    if (p->is_temp())
      continue;
    if (p->is_meta())
      continue;
    coll_t temp = p->get_temp();
    if (temps.count(temp)) {
      temps.erase(temp);
    } else {
      dout(10) << __func__ << " creating " << temp << dendl;
      r = _create_collection(temp, spos);
      assert(r == 0);
    }
  }

  for (set<coll_t>::iterator p = temps.begin(); p != temps.end(); ++p) {
    dout(10) << __func__ << " removing stray " << *p << dendl;
    r = _collection_remove_recursive(*p, spos);
    assert(r == 0);
  }
}

int CStore::umount()
{
  dout(5) << "umount " << basedir << dendl;

  flush();
  sync();
  do_force_sync();

  lock.Lock();
  stop = true;
  sync_cond.Signal();
  lock.Unlock();
  sync_thread.join();
  if (!m_disable_wbthrottle){
    wbthrottle.stop();
  }

	{
		Mutex::Locker l(timer_lock);
	  timer.cancel_all_events();
	}

	hit_set_clear();

  op_tp.stop();

  comp_tp.stop();

  journal_stop();
  if (!(generic_flags & SKIP_JOURNAL_REPLAY))
    journal_write_close();

  for (vector<Finisher*>::iterator it = ondisk_finishers.begin(); it != ondisk_finishers.end(); ++it) {
    (*it)->stop();
  }
  for (vector<Finisher*>::iterator it = apply_finishers.begin(); it != apply_finishers.end(); ++it) {
    (*it)->stop();
  }

  if (fsid_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(fsid_fd));
    fsid_fd = -1;
  }
  if (op_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(op_fd));
    op_fd = -1;
  }
  if (current_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(current_fd));
    current_fd = -1;
  }
  if (basedir_fd >= 0) {
    VOID_TEMP_FAILURE_RETRY(::close(basedir_fd));
    basedir_fd = -1;
  }

  force_sync = false;

  delete backend;
  backend = NULL;

  object_map.reset();

  {
    Mutex::Locker l(timer_lock);
    timer.shutdown();
  }

  // nothing
  return 0;
}




/// -----------------------------

CStore::Op *CStore::build_op(vector<Transaction>& tls,
				   Context *onreadable,
				   Context *onreadable_sync,
				   TrackedOpRef osd_op)
{
  uint64_t bytes = 0, ops = 0;
  for (vector<Transaction>::iterator p = tls.begin();
       p != tls.end();
       ++p) {
    bytes += (*p).get_num_bytes();
    ops += (*p).get_num_ops();
  }

  Op *o = new Op;
  o->start = ceph_clock_now(g_ceph_context);
  o->tls = std::move(tls);
  o->onreadable = onreadable;
  o->onreadable_sync = onreadable_sync;
  o->ops = ops;
  o->bytes = bytes;
  o->osd_op = osd_op;
  return o;
}



void CStore::queue_op(OpSequencer *osr, Op *o)
{
  // queue op on sequencer, then queue sequencer for the threadpool,
  // so that regardless of which order the threads pick up the
  // sequencer, the op order will be preserved.

  osr->queue(o);

  logger->inc(l_os_ops);
  logger->inc(l_os_bytes, o->bytes);

  dout(5) << "queue_op " << o << " seq " << o->op
	  << " " << *osr
	  << " " << o->bytes << " bytes"
	  << "   (queue has " << throttle_ops.get_current() << " ops and " << throttle_bytes.get_current() << " bytes)"
	  << dendl;
  op_wq.queue(osr);
}

void CStore::op_queue_reserve_throttle(Op *o)
{
  throttle_ops.get();
  throttle_bytes.get(o->bytes);

  logger->set(l_os_oq_ops, throttle_ops.get_current());
  logger->set(l_os_oq_bytes, throttle_bytes.get_current());
}

void CStore::op_queue_release_throttle(Op *o)
{
  throttle_ops.put();
  throttle_bytes.put(o->bytes);
  logger->set(l_os_oq_ops, throttle_ops.get_current());
  logger->set(l_os_oq_bytes, throttle_bytes.get_current());
}

void CStore::_do_op(OpSequencer *osr, ThreadPool::TPHandle &handle)
{
  if (!m_disable_wbthrottle) {
    wbthrottle.throttle();
  }
  // inject a stall?
  if (g_conf->filestore_inject_stall) {
    int orig = g_conf->filestore_inject_stall;
    dout(5) << "_do_op filestore_inject_stall " << orig << ", sleeping" << dendl;
    for (int n = 0; n < g_conf->filestore_inject_stall; n++)
      sleep(1);
    g_conf->set_val("filestore_inject_stall", "0");
    dout(5) << "_do_op done stalling" << dendl;
  }

  osr->apply_lock.Lock();
  Op *o = osr->peek_queue();
  o->deq = ceph_clock_now(g_ceph_context);
  apply_manager.op_apply_start(o->op);
  dout(5) << "_do_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << " start" << dendl;
  int r = _do_transactions(o->tls, o->op, &handle);
  apply_manager.op_apply_finish(o->op);
  dout(10) << "_do_op " << o << " seq " << o->op << " r = " << r
	   << ", finisher " << o->onreadable << " " << o->onreadable_sync << dendl;

  o->tls.clear();

}

void CStore::_finish_op(OpSequencer *osr)
{
  list<Context*> to_queue;
  Op *o = osr->dequeue(&to_queue);
  osr->apply_lock.Unlock();  // locked in _do_op

  // called with tp lock held
  op_queue_release_throttle(o);

  utime_t lat = ceph_clock_now(g_ceph_context);

  dout(10) << "_finish_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << " lat " << lat << dendl;

  logger->tinc(l_os_apply_lat, lat - o->start);
  logger->tinc(l_os_op_lat, lat - o->deq);

  if (o->onreadable_sync) {
    o->onreadable_sync->complete(0);
  }
  if (o->onreadable) {
    apply_finishers[osr->id % m_apply_finisher_num]->queue(o->onreadable);
  }
  if (!to_queue.empty()) {
    apply_finishers[osr->id % m_apply_finisher_num]->queue(to_queue);
  }
  delete o;
}


struct C_CStoreJournaledAhead : public Context {
  CStore *fs;
  CStore::OpSequencer *osr;
  CStore::Op *o;
  Context *ondisk;

  C_CStoreJournaledAhead(CStore *f, CStore::OpSequencer *os, CStore::Op *o, Context *ondisk):
    fs(f), osr(os), o(o), ondisk(ondisk) { }
  void finish(int r) {
    fs->_journaled_ahead(osr, o, ondisk);
  }
};

int CStore::queue_transactions(Sequencer *posr, vector<Transaction>& tls,
				  TrackedOpRef osd_op,
				  ThreadPool::TPHandle *handle)
{
  Context *onreadable;
  Context *ondisk;
  Context *onreadable_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &onreadable, &ondisk, &onreadable_sync);
  if (g_conf->filestore_blackhole) {
    dout(0) << "queue_transactions filestore_blackhole = TRUE, dropping transaction" << dendl;
    delete ondisk;
    delete onreadable;
    delete onreadable_sync;
    return 0;
  }

  utime_t start = ceph_clock_now(g_ceph_context);
  // set up the sequencer
  OpSequencer *osr;
  assert(posr);
  if (posr->p) {
    osr = static_cast<OpSequencer *>(posr->p.get());
    dout(5) << "queue_transactions existing " << osr << " " << *osr << dendl;
  } else {
    osr = new OpSequencer(next_osr_id.inc());
    osr->set_cct(g_ceph_context);
    osr->parent = posr;
    posr->p = osr;
    dout(5) << "queue_transactions new " << osr << " " << *osr << dendl;
  }

  // used to include osr information in tracepoints during transaction apply
  for (vector<Transaction>::iterator i = tls.begin(); i != tls.end(); ++i) {
    (*i).set_osr(osr);
  }

  if (journal && journal->is_writeable() && !m_filestore_journal_trailing) {
    Op *o = build_op(tls, onreadable, onreadable_sync, osd_op);

    //prepare and encode transactions data out of lock
    bufferlist tbl;
    int orig_len = journal->prepare_entry(o->tls, &tbl);

    if (handle)
      handle->suspend_tp_timeout();

    op_queue_reserve_throttle(o);
    journal->reserve_throttle_and_backoff(tbl.length());

    if (handle)
      handle->reset_tp_timeout();

    uint64_t op_num = submit_manager.op_submit_start();
    o->op = op_num;

    if (m_filestore_do_dump)
      dump_transactions(o->tls, o->op, osr);

    if (m_filestore_journal_parallel) {
      dout(5) << "queue_transactions (parallel) " << o->op << " " << o->tls << dendl;

      _op_journal_transactions(tbl, orig_len, o->op, ondisk, osd_op);

      // queue inside submit_manager op submission lock
      queue_op(osr, o);
    } else if (m_filestore_journal_writeahead) {
      dout(5) << "queue_transactions (writeahead) " << o->op << " " << o->tls << dendl;

      osr->queue_journal(o->op);

      _op_journal_transactions(tbl, orig_len, o->op,
			       new C_CStoreJournaledAhead(this, osr, o, ondisk),
			       osd_op);
    } else {
      assert(0);
    }
    submit_manager.op_submit_finish(op_num);
    utime_t end = ceph_clock_now(g_ceph_context);
    logger->tinc(l_os_queue_lat, end - start);
    return 0;
  }

  if (!journal) {
    Op *o = build_op(tls, onreadable, onreadable_sync, osd_op);
    dout(5) << __func__ << " (no journal) " << o << " " << tls << dendl;

    if (handle)
      handle->suspend_tp_timeout();

    op_queue_reserve_throttle(o);

    if (handle)
      handle->reset_tp_timeout();

    uint64_t op_num = submit_manager.op_submit_start();
    o->op = op_num;

    if (m_filestore_do_dump)
      dump_transactions(o->tls, o->op, osr);

    queue_op(osr, o);

    if (ondisk)
      apply_manager.add_waiter(op_num, ondisk);
    submit_manager.op_submit_finish(op_num);
    utime_t end = ceph_clock_now(g_ceph_context);
    logger->tinc(l_os_queue_lat, end - start);
    return 0;
  }

  assert(journal);
  //prepare and encode transactions data out of lock
  bufferlist tbl;
  int orig_len = -1;
  if (journal->is_writeable()) {
    orig_len = journal->prepare_entry(tls, &tbl);
  }
  uint64_t op = submit_manager.op_submit_start();
  dout(5) << "queue_transactions (trailing journal) " << op << " " << tls << dendl;

  if (m_filestore_do_dump)
    dump_transactions(tls, op, osr);

  apply_manager.op_apply_start(op);
  int r = do_transactions(tls, op);

  if (r >= 0) {
    _op_journal_transactions(tbl, orig_len, op, ondisk, osd_op);
  } else {
    delete ondisk;
  }

  // start on_readable finisher after we queue journal item, as on_readable callback
  // is allowed to delete the Transaction
  if (onreadable_sync) {
    onreadable_sync->complete(r);
  }
  apply_finishers[osr->id % m_apply_finisher_num]->queue(onreadable, r);

  submit_manager.op_submit_finish(op);
  apply_manager.op_apply_finish(op);

  utime_t end = ceph_clock_now(g_ceph_context);
  logger->tinc(l_os_queue_lat, end - start);
  return r;
}

void CStore::_journaled_ahead(OpSequencer *osr, Op *o, Context *ondisk)
{
  dout(5) << "_journaled_ahead " << o << " seq " << o->op << " " << *osr << " " << o->tls << dendl;

  // this should queue in order because the journal does it's completions in order.
  queue_op(osr, o);

  list<Context*> to_queue;
  osr->dequeue_journal(&to_queue);

  // do ondisk completions async, to prevent any onreadable_sync completions
  // getting blocked behind an ondisk completion.
  if (ondisk) {
    dout(10) << " queueing ondisk " << ondisk << dendl;
    ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(ondisk);
  }
  if (!to_queue.empty()) {
    ondisk_finishers[osr->id % m_ondisk_finisher_num]->queue(to_queue);
  }
}

int CStore::_do_transactions(
  vector<Transaction> &tls,
  uint64_t op_seq,
  ThreadPool::TPHandle *handle)
{
  int trans_num = 0;

  for (vector<Transaction>::iterator p = tls.begin();
       p != tls.end();
       ++p, trans_num++)
    _do_transaction(*p, op_seq, trans_num, handle);

  return 0;
}

void CStore::_set_global_replay_guard(const coll_t& cid,
					 const CStoreSequencerPosition &spos)
{
  if (backend->can_checkpoint())
    return;

  // sync all previous operations on this sequencer
  int ret = object_map->sync();
  if (ret < 0) {
    derr << __func__ << " : omap sync error " << cpp_strerror(ret) << dendl;
    assert(0 == "_set_global_replay_guard failed");
  }
  ret = sync_filesystem(basedir_fd);
  if (ret < 0) {
    derr << __func__ << " : sync_filesytem error " << cpp_strerror(ret) << dendl;
    assert(0 == "_set_global_replay_guard failed");
  }

  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    int err = errno;
    derr << __func__ << ": " << cid << " error " << cpp_strerror(err) << dendl;
    assert(0 == "_set_global_replay_guard failed");
  }

  _inject_failure();

  // then record that we did it
  bufferlist v;
  ::encode(spos, v);
  int r = chain_fsetxattr<true, true>(
    fd, GLOBAL_REPLAY_GUARD_XATTR, v.c_str(), v.length());
  if (r < 0) {
    derr << __func__ << ": fsetxattr " << GLOBAL_REPLAY_GUARD_XATTR
	 << " got " << cpp_strerror(r) << dendl;
    assert(0 == "fsetxattr failed");
  }

  // and make sure our xattr is durable.
  ::fsync(fd);

  _inject_failure();

  VOID_TEMP_FAILURE_RETRY(::close(fd));
  dout(10) << __func__ << ": " << spos << " done" << dendl;
}

int CStore::_check_global_replay_guard(const coll_t& cid,
					  const CStoreSequencerPosition& spos)
{
  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    dout(10) << __func__ << ": " << cid << " dne" << dendl;
    return 1;  // if collection does not exist, there is no guard, and we can replay.
  }

  char buf[100];
  int r = chain_fgetxattr(fd, GLOBAL_REPLAY_GUARD_XATTR, buf, sizeof(buf));
  if (r < 0) {
    dout(20) << __func__ << " no xattr" << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return 1;  // no xattr
  }
  bufferlist bl;
  bl.append(buf, r);

  CStoreSequencerPosition opos;
  bufferlist::iterator p = bl.begin();
  ::decode(opos, p);

  VOID_TEMP_FAILURE_RETRY(::close(fd));
  return spos >= opos ? 1 : -1;
}


void CStore::_set_replay_guard(const coll_t& cid,
                                  const CStoreSequencerPosition &spos,
                                  bool in_progress=false)
{
  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    int err = errno;
    derr << "_set_replay_guard " << cid << " error " << cpp_strerror(err) << dendl;
    assert(0 == "_set_replay_guard failed");
  }
  _set_replay_guard(fd, spos, 0, in_progress);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
}


void CStore::_set_replay_guard(int fd,
				  const CStoreSequencerPosition& spos,
				  const ghobject_t *hoid,
				  bool in_progress)
{
  if (backend->can_checkpoint())
    return;

  dout(10) << "_set_replay_guard " << spos << (in_progress ? " START" : "") << dendl;

  _inject_failure();

  // first make sure the previous operation commits
  ::fsync(fd);

  // sync object_map too.  even if this object has a header or keys,
  // it have had them in the past and then removed them, so always
  // sync.
  object_map->sync(hoid, &spos);

  _inject_failure();

  // then record that we did it
  bufferlist v(40);
  ::encode(spos, v);
  ::encode(in_progress, v);
  int r = chain_fsetxattr<true, true>(
    fd, REPLAY_GUARD_XATTR, v.c_str(), v.length());
  if (r < 0) {
    derr << "fsetxattr " << REPLAY_GUARD_XATTR << " got " << cpp_strerror(r) << dendl;
    assert(0 == "fsetxattr failed");
  }

  // and make sure our xattr is durable.
  ::fsync(fd);

  _inject_failure();

  dout(10) << "_set_replay_guard " << spos << " done" << dendl;
}

void CStore::_close_replay_guard(const coll_t& cid,
                                    const CStoreSequencerPosition &spos)
{
  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    int err = errno;
    derr << "_close_replay_guard " << cid << " error " << cpp_strerror(err) << dendl;
    assert(0 == "_close_replay_guard failed");
  }
  _close_replay_guard(fd, spos);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
}

void CStore::_close_replay_guard(int fd, const CStoreSequencerPosition& spos)
{
  if (backend->can_checkpoint())
    return;

  dout(10) << "_close_replay_guard " << spos << dendl;

  _inject_failure();

  // then record that we are done with this operation
  bufferlist v(40);
  ::encode(spos, v);
  bool in_progress = false;
  ::encode(in_progress, v);
  int r = chain_fsetxattr<true, true>(
    fd, REPLAY_GUARD_XATTR, v.c_str(), v.length());
  if (r < 0) {
    derr << "fsetxattr " << REPLAY_GUARD_XATTR << " got " << cpp_strerror(r) << dendl;
    assert(0 == "fsetxattr failed");
  }

  // and make sure our xattr is durable.
  ::fsync(fd);

  _inject_failure();

  dout(10) << "_close_replay_guard " << spos << " done" << dendl;
}

int CStore::_check_replay_guard(const coll_t& cid, ghobject_t oid, const CStoreSequencerPosition& spos)
{
  if (!replaying || backend->can_checkpoint())
    return 1;

  int r = _check_global_replay_guard(cid, spos);
  if (r < 0)
    return r;

  CFDRef fd;
  r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    dout(10) << "_check_replay_guard " << cid << " " << oid << " dne" << dendl;
    return 1;  // if file does not exist, there is no guard, and we can replay.
  }
  int ret = _check_replay_guard(**fd, spos);
  lfn_close(fd);
  return ret;
}

int CStore::_check_replay_guard(const coll_t& cid, const CStoreSequencerPosition& spos)
{
  if (!replaying || backend->can_checkpoint())
    return 1;

  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    dout(10) << "_check_replay_guard " << cid << " dne" << dendl;
    return 1;  // if collection does not exist, there is no guard, and we can replay.
  }
  int ret = _check_replay_guard(fd, spos);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  return ret;
}

int CStore::_check_replay_guard(int fd, const CStoreSequencerPosition& spos)
{
  if (!replaying || backend->can_checkpoint())
    return 1;

  char buf[100];
  int r = chain_fgetxattr(fd, REPLAY_GUARD_XATTR, buf, sizeof(buf));
  if (r < 0) {
    dout(20) << "_check_replay_guard no xattr" << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    return 1;  // no xattr
  }
  bufferlist bl;
  bl.append(buf, r);

  CStoreSequencerPosition opos;
  bufferlist::iterator p = bl.begin();
  ::decode(opos, p);
  bool in_progress = false;
  if (!p.end())   // older journals don't have this
    ::decode(in_progress, p);
  if (opos > spos) {
    dout(10) << "_check_replay_guard object has " << opos << " > current pos " << spos
	     << ", now or in future, SKIPPING REPLAY" << dendl;
    return -1;
  } else if (opos == spos) {
    if (in_progress) {
      dout(10) << "_check_replay_guard object has " << opos << " == current pos " << spos
	       << ", in_progress=true, CONDITIONAL REPLAY" << dendl;
      return 0;
    } else {
      dout(10) << "_check_replay_guard object has " << opos << " == current pos " << spos
	       << ", in_progress=false, SKIPPING REPLAY" << dendl;
      return -1;
    }
  } else {
    dout(10) << "_check_replay_guard object has " << opos << " < current pos " << spos
	     << ", in past, will replay" << dendl;
    return 1;
  }
}

void CStore::_do_transaction(
  Transaction& t, uint64_t op_seq, int trans_num,
  ThreadPool::TPHandle *handle)
{
  dout(10) << "_do_transaction on " << &t << dendl;

#ifdef WITH_LTTNG
  const char *osr_name = t.get_osr() ? static_cast<OpSequencer*>(t.get_osr())->get_name().c_str() : "<NULL>";
#endif

  Transaction::iterator i = t.begin();

  CStoreSequencerPosition spos(op_seq, trans_num, 0);
  while (i.have_op()) {
    if (handle)
      handle->reset_tp_timeout();

    Transaction::Op *op = i.decode_op();
    int r = 0;

		// if object is involved in this op
		// track the object
		if (is_object_op(op)) {
			Mutex::Locker l(hit_set_lock);
			ghobject_t oid = i.get_oid(op->oid);
			assert(oid != ghobject_t());
		  hit_set->insert(oid.hobj);
			dout(20) << __func__ << " hit_set full " << hit_set->is_full() << " count " << hit_set->approx_unique_insert_count() << dendl;
		  if(hit_set->is_full() || hit_set_timeout()) 
			  hit_set_persist();
		}

    _inject_failure();

    switch (op->op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        tracepoint(objectstore, touch_enter, osr_name);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _touch(cid, oid);
        tracepoint(objectstore, touch_exit, r);
      }
      break;

    case Transaction::OP_WRITE:
      {
        utime_t s = ceph_clock_now(g_ceph_context);
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
        tracepoint(objectstore, write_enter, osr_name, off, len);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _write(cid, oid, off, len, bl, fadvise_flags);
        tracepoint(objectstore, write_exit, r);
        utime_t t = ceph_clock_now(g_ceph_context) - s;
        logger->tinc(l_os_write_lat, t);
      }
      break;

    case Transaction::OP_ZERO:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        tracepoint(objectstore, zero_enter, osr_name, off, len);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _zero(cid, oid, off, len);
        tracepoint(objectstore, zero_exit, r);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
	// deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        uint64_t off = op->off;
        tracepoint(objectstore, truncate_enter, osr_name, off);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _truncate(cid, oid, off);
        tracepoint(objectstore, truncate_exit, r);
      }
      break;

    case Transaction::OP_REMOVE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        tracepoint(objectstore, remove_enter, osr_name);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _remove(cid, oid, spos);
        tracepoint(objectstore, remove_exit, r);
      }
      break;

    case Transaction::OP_SETATTR:
      {
        utime_t s = ceph_clock_now(g_ceph_context);
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
        tracepoint(objectstore, setattr_enter, osr_name);
        if (_check_replay_guard(cid, oid, spos) > 0) {
          map<string, bufferptr> to_set;
          to_set[name] = bufferptr(bl.c_str(), bl.length());
          r = _setattrs(cid, oid, to_set, spos);
          if (r == -ENOSPC)
            dout(0) << " ENOSPC on setxattr on " << cid << "/" << oid
                    << " name " << name << " size " << bl.length() << dendl;
        }
        tracepoint(objectstore, setattr_exit, r);
        utime_t t = ceph_clock_now(g_ceph_context) - s;
        logger->tinc(l_os_setattr_lat, t);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        utime_t s = ceph_clock_now(g_ceph_context);
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
        tracepoint(objectstore, setattrs_enter, osr_name);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _setattrs(cid, oid, aset, spos);
        tracepoint(objectstore, setattrs_exit, r);
        if (r == -ENOSPC)
          dout(0) << " ENOSPC on setxattrs on " << cid << "/" << oid << dendl;
        utime_t t = ceph_clock_now(g_ceph_context) - s;
        logger->tinc(l_os_setattrs_lat, t);
      }
      break;

    case Transaction::OP_RMATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        string name = i.decode_string();
        tracepoint(objectstore, rmattr_enter, osr_name);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _rmattr(cid, oid, name.c_str(), spos);
        tracepoint(objectstore, rmattr_exit, r);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        tracepoint(objectstore, rmattrs_enter, osr_name);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _rmattrs(cid, oid, spos);
        tracepoint(objectstore, rmattrs_exit, r);
      }
      break;

    case Transaction::OP_CLONE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
        tracepoint(objectstore, clone_enter, osr_name);
        r = _clone(cid, oid, noid, spos);
        tracepoint(objectstore, clone_exit, r);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
	_kludge_temp_object_collection(cid, noid);
        uint64_t off = op->off;
        uint64_t len = op->len;
        tracepoint(objectstore, clone_range_enter, osr_name, len);
        r = _clone_range(cid, oid, noid, off, len, off, spos);
        tracepoint(objectstore, clone_range_exit, r);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        ghobject_t noid = i.get_oid(op->dest_oid);
	_kludge_temp_object_collection(cid, noid);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
        tracepoint(objectstore, clone_range2_enter, osr_name, len);
        r = _clone_range(cid, oid, noid, srcoff, len, dstoff, spos);
        tracepoint(objectstore, clone_range2_exit, r);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        tracepoint(objectstore, mkcoll_enter, osr_name);
        if (_check_replay_guard(cid, spos) > 0)
          r = _create_collection(cid, spos);
        tracepoint(objectstore, mkcoll_exit, r);
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t type = op->hint_type;
        bufferlist hint;
        i.decode_bl(hint);
        bufferlist::iterator hiter = hint.begin();
        if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
          uint32_t pg_num;
          uint64_t num_objs;
          ::decode(pg_num, hiter);
          ::decode(num_objs, hiter);
          if (_check_replay_guard(cid, spos) > 0) {
            r = _collection_hint_expected_num_objs(cid, pg_num, num_objs, spos);
          }
        } else {
          // Ignore the hint
          dout(10) << "Unrecognized collection hint type: " << type << dendl;
        }
      }
      break;

    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
        tracepoint(objectstore, rmcoll_enter, osr_name);
        if (_check_replay_guard(cid, spos) > 0)
          r = _destroy_collection(cid);
        tracepoint(objectstore, rmcoll_exit, r);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
        coll_t ocid = i.get_cid(op->cid);
        coll_t ncid = i.get_cid(op->dest_cid);
        ghobject_t oid = i.get_oid(op->oid);

	assert(oid.hobj.pool >= -1);

        // always followed by OP_COLL_REMOVE
        Transaction::Op *op2 = i.decode_op();
        coll_t ocid2 = i.get_cid(op2->cid);
        ghobject_t oid2 = i.get_oid(op2->oid);
        assert(op2->op == Transaction::OP_COLL_REMOVE);
        assert(ocid2 == ocid);
        assert(oid2 == oid);

        tracepoint(objectstore, coll_add_enter);
        r = _collection_add(ncid, ocid, oid, spos);
        tracepoint(objectstore, coll_add_exit, r);
        spos.op++;
        if (r < 0)
          break;
        tracepoint(objectstore, coll_remove_enter, osr_name);
        if (_check_replay_guard(ocid, oid, spos) > 0)
          r = _remove(ocid, oid, spos);
        tracepoint(objectstore, coll_remove_exit, r);
      }
      break;

    case Transaction::OP_COLL_MOVE:
      {
        // WARNING: this is deprecated and buggy; only here to replay old journals.
        coll_t ocid = i.get_cid(op->cid);
        coll_t ncid = i.get_cid(op->dest_cid);
        ghobject_t oid = i.get_oid(op->oid);
        tracepoint(objectstore, coll_move_enter);
        r = _collection_add(ocid, ncid, oid, spos);
        if (r == 0 &&
            (_check_replay_guard(ocid, oid, spos) > 0))
          r = _remove(ocid, oid, spos);
        tracepoint(objectstore, coll_move_exit, r);
      }
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
        ghobject_t oldoid = i.get_oid(op->oid);
        coll_t newcid = i.get_cid(op->dest_cid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	_kludge_temp_object_collection(oldcid, oldoid);
	_kludge_temp_object_collection(newcid, newoid);
        tracepoint(objectstore, coll_move_rename_enter);
        r = _collection_move_rename(oldcid, oldoid, newcid, newoid, spos);
        tracepoint(objectstore, coll_move_rename_exit, r);
      }
      break;

    case Transaction::OP_TRY_RENAME:
      {
        coll_t oldcid = i.get_cid(op->cid);
	coll_t newcid = oldcid;
        ghobject_t oldoid = i.get_oid(op->oid);
        ghobject_t newoid = i.get_oid(op->dest_oid);
	_kludge_temp_object_collection(oldcid, oldoid);
	_kludge_temp_object_collection(newcid, newoid);
        tracepoint(objectstore, coll_try_rename_enter);
        r = _collection_move_rename(oldcid, oldoid, newcid, newoid, spos, true);
        tracepoint(objectstore, coll_try_rename_exit, r);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
        tracepoint(objectstore, coll_setattr_enter, osr_name);
        if (_check_replay_guard(cid, spos) > 0)
          r = _collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
        tracepoint(objectstore, coll_setattr_exit, r);
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
        coll_t cid = i.get_cid(op->cid);
        string name = i.decode_string();
        tracepoint(objectstore, coll_rmattr_enter, osr_name);
        if (_check_replay_guard(cid, spos) > 0)
          r = _collection_rmattr(cid, name.c_str());
        tracepoint(objectstore, coll_rmattr_exit, r);
      }
      break;

    case Transaction::OP_STARTSYNC:
      tracepoint(objectstore, startsync_enter, osr_name);
      _start_sync();
      tracepoint(objectstore, startsync_exit);
      break;

    case Transaction::OP_COLL_RENAME:
      {
        r = -EOPNOTSUPP;
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        tracepoint(objectstore, omap_clear_enter, osr_name);
        r = _omap_clear(cid, oid, spos);
        tracepoint(objectstore, omap_clear_exit, r);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
        utime_t s = ceph_clock_now(g_ceph_context);
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        map<string, bufferlist> aset;
        i.decode_attrset(aset);
        tracepoint(objectstore, omap_setkeys_enter, osr_name);
        r = _omap_setkeys(cid, oid, aset, spos);
        tracepoint(objectstore, omap_setkeys_exit, r);
        utime_t t = ceph_clock_now(g_ceph_context) - s;
        logger->tinc(l_os_setomap_lat, t);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        set<string> keys;
        i.decode_keyset(keys);
        tracepoint(objectstore, omap_rmkeys_enter, osr_name);
        r = _omap_rmkeys(cid, oid, keys, spos);
        tracepoint(objectstore, omap_rmkeys_exit, r);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
        tracepoint(objectstore, omap_rmkeyrange_enter, osr_name);
        r = _omap_rmkeyrange(cid, oid, first, last, spos);
        tracepoint(objectstore, omap_rmkeyrange_exit, r);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        bufferlist bl;
        i.decode_bl(bl);
        tracepoint(objectstore, omap_setheader_enter, osr_name);
        r = _omap_setheader(cid, oid, bl, spos);
        tracepoint(objectstore, omap_setheader_exit, r);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      {
	assert(0 == "not legacy journal; upgrade to firefly first");
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
        coll_t cid = i.get_cid(op->cid);
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
        coll_t dest = i.get_cid(op->dest_cid);
        tracepoint(objectstore, split_coll2_enter, osr_name);
        r = _split_collection(cid, bits, rem, dest, spos);
        tracepoint(objectstore, split_coll2_exit, r);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        coll_t cid = i.get_cid(op->cid);
        ghobject_t oid = i.get_oid(op->oid);
	_kludge_temp_object_collection(cid, oid);
        uint64_t expected_object_size = op->expected_object_size;
        uint64_t expected_write_size = op->expected_write_size;
        tracepoint(objectstore, setallochint_enter, osr_name);
        if (_check_replay_guard(cid, oid, spos) > 0)
          r = _set_alloc_hint(cid, oid, expected_object_size,
                              expected_write_size);
        tracepoint(objectstore, setallochint_exit, r);
      }
      break;

    default:
      derr << "bad op " << op->op << dendl;
      assert(0);
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// -ENOENT is normally okay
	// ...including on a replayed OP_RMCOLL with checkpoint mode
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (op->op == Transaction::OP_SETALLOCHINT)
        // Either EOPNOTSUPP or EINVAL most probably.  EINVAL in most
        // cases means invalid hint size (e.g. too big, not a multiple
        // of block size, etc) or, at least on xfs, an attempt to set
        // or change it when the file is not empty.  However,
        // OP_SETALLOCHINT is advisory, so ignore all errors.
        ok = true;

      if (replaying && !backend->can_checkpoint()) {
	if (r == -EEXIST && op->op == Transaction::OP_MKCOLL) {
	  dout(10) << "tolerating EEXIST during journal replay since checkpoint is not enabled" << dendl;
	  ok = true;
	}
	if (r == -EEXIST && op->op == Transaction::OP_COLL_ADD) {
	  dout(10) << "tolerating EEXIST during journal replay since checkpoint is not enabled" << dendl;
	  ok = true;
	}
	if (r == -EEXIST && op->op == Transaction::OP_COLL_MOVE) {
	  dout(10) << "tolerating EEXIST during journal replay since checkpoint is not enabled" << dendl;
	  ok = true;
	}
	if (r == -ERANGE) {
	  dout(10) << "tolerating ERANGE on replay" << dendl;
	  ok = true;
	}
	if (r == -ENOENT) {
	  dout(10) << "tolerating ENOENT on replay" << dendl;
	  ok = true;
	}
      }

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC handling not implemented";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op
		<< " (" << spos << ", or op " << spos.op << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t.dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;

	if (r == -EMFILE) {
	  dump_open_fds(g_ceph_context);
	}

	assert(0 == "unexpected error");
      }
    }

    spos.op++;
  }

  _inject_failure();
}

  /*********************************************/



// --------------------
// objects

bool CStore::exists(const coll_t& _cid, const ghobject_t& oid)
{
  tracepoint(objectstore, exists_enter, _cid.c_str());
  const coll_t& cid = !_need_temp_object_collection(_cid, oid) ? _cid : _cid.get_temp();
  struct stat st;
  bool retval = stat(cid, oid, &st) == 0;
  tracepoint(objectstore, exists_exit, retval);
  return retval;
}

int CStore::stat(
  const coll_t& _cid, const ghobject_t& oid, struct stat *st, bool allow_eio)
{
  tracepoint(objectstore, stat_enter, _cid.c_str());
  const coll_t& cid = !_need_temp_object_collection(_cid, oid) ? _cid : _cid.get_temp();

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

	int r;
  set<string> keys;
  map<string, bufferlist> values;
  ObjnodeRef obj;
  objnode_t *node = new objnode_t(oid, m_block_size, 0);;
  keys.insert(OBJ_DATA);
  r = object_map->get_xattrs(oid, keys, &values);
  if (r < 0) {
    dout(0) << __func__ << " cannot find object data " << oid << dendl;
    goto out;
  } else {
    bufferlist::iterator p = values[OBJ_DATA].begin();
    ::decode(*node, p);
    obj.reset(node);
  }

  if (obj->is_compressed()) {
    r = _decompress(_cid, oid, obj);
    if (r < 0) {
      derr << " decompress error " << dendl;
      goto out;
    }
  } 

  r = lfn_stat(cid, oid, st);
  assert(allow_eio || !m_filestore_fail_eio || r != -EIO);
  if (r < 0) {
    dout(10) << "stat " << cid << "/" << oid
	     << " = " << r << dendl;
  } else {
    dout(10) << "stat " << cid << "/" << oid
	     << " = " << r
	     << " (size " << st->st_size << ")" << dendl;
  }
  if (g_conf->filestore_debug_inject_read_err &&
      debug_mdata_eio(oid)) {
    r = -EIO;
  } else {
    tracepoint(objectstore, stat_exit, r);
  }

out:
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

	return r;
}

int CStore::_replay_event(const coll_t &cid, const ghobject_t &oid, uint8_t state, bool compression) {
  dout(20) << __func__ << " replay object " << oid << " compression " << compression << dendl;

  set<string> keys;
  struct stat st;
  string key;
  bufferlist bl;
  map<string, bufferlist> vals;
  ghobject_t coid;
  objnode_t *node;
  string prefix;
  objnode_t::state_t ctype;
  int r;
  CFDRef fd, cfd;
  char buf[2];
  map<string, bufferptr> aset;

  compression_header_t h(cid, oid, (compression_header_t::state_t)state);

  if (compression) {
    coid = h.oid.make_temp_obj(NS_COMPRESS);
    prefix = PREFIX_COMPRESS;
    // FIXME
    ctype = objnode_t::COMP_ALG_SNAPPY;
  } else {
    coid = h.oid.make_temp_obj(NS_DECOMPRESS);
    prefix = PREFIX_DECOMPRESS;
    ctype = objnode_t::COMP_ALG_NONE;
  }

  keys.insert(OBJ_DATA);
  r = object_map->get_xattrs(oid, keys, &vals);

  assert(r == 0);

  node = new objnode_t(oid, m_block_size, 0);
  bufferlist::iterator p = vals[OBJ_DATA].begin();
  ::decode(*node, p);
  keys.clear();
  vals.clear();

  get_object_key(oid, &key);
  r = lfn_stat(cid, oid, &st);
  dout(20) << __func__ << " temp obj stat r = " << r << dendl;

  if (r == -ENOENT) {
    // non-exist temp object, clear WAL
    keys.insert(key);
    if (h.state == compression_header_t::STATE_INIT) {
      assert(!node->is_compressed());
      r = object_map->rm_keys_by_prefix(prefix, keys);   
    } else if (h.state == compression_header_t::STATE_PROGRESS) {
      assert(node->is_compressed());
      r = object_map->rm_keys_by_prefix(prefix, keys);   
    }
    keys.clear();
    if (r < 0) {
      derr << __func__ << " clear wal error " << dendl;
      goto out;
    }

    r = object_map->sync();
    if (r < 0) {
      derr << __func__ << " sync wal & metadata error " << dendl;
      goto out;
    }
  } else if (r == 0) {
    // temp object exist
    if (h.state == compression_header_t::STATE_INIT) {
      // copy attres
      r = lfn_open(cid, oid, false, &fd);
      if (r < 0) {
        derr << "cannot open " << oid << dendl;
        goto out1;
      }

      r = _fgetattrs(**fd, aset);
      if (r < 0) {
	derr << "cannot get extend attrs " << cpp_strerror(r) << dendl;
	goto out1;
      }

      r = chain_fgetxattr(**fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
      if (r < 0) {
	derr << "cannot get spill attr " << cpp_strerror(r) << dendl;
	goto out1;
      }

      r = lfn_open(cid, coid, true, &cfd);
      if (r < 0) {
        derr << "cannot open " << coid << dendl;
        goto out2;
      }

      if (!strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
	r = chain_fsetxattr<true, true>(**cfd, XATTR_SPILL_OUT_NAME, XATTR_NO_SPILL_OUT,
			    sizeof(XATTR_NO_SPILL_OUT));
      } else {
	r = chain_fsetxattr<true, true>(**cfd, XATTR_SPILL_OUT_NAME, XATTR_SPILL_OUT,
			    sizeof(XATTR_SPILL_OUT));
      }
      if (r < 0) {
        derr << __func__ << " set spillout error" << dendl;
        goto out2;
      }

      r = _fsetattrs(**cfd, aset);
      if (r < 0) {
	derr << __func__ << " _fsetattrs error " << dendl;
	goto out2;
      }

      r = ::fsync(**cfd);
      if (r < 0) {
	derr << __func__ << " fsync " << coid << " error " << dendl;
	goto out2;
      }

      // update object metadata
      node->c_type = ctype;
      ::encode(*node, bl);
      vals[OBJ_DATA] = bl;
      r = object_map->set_xattrs(oid, vals);
      if (r < 0) {
	derr << __func__ << " set metadata " << oid << dendl;
	goto out;
      }
      bl.clear();
      vals.clear();

      h.state = compression_header_t::STATE_INIT;
      ::encode(h, bl);
      vals[key] = bl;
      r = object_map->set_keys_by_prefix(prefix, vals);
      if (r < 0) {
	derr << __func__ << " set wal " << oid << dendl;
	goto out;
      }
      bl.clear();
      vals.clear();

      r = object_map->sync();
      if (r < 0) {
	derr << __func__ << " sync wal & metadata error " << dendl;
	goto out;
      }

      r = lfn_unlink(h.cid, oid, false);

      r = lfn_link(h.cid, h.cid, coid, h.oid);

      r = lfn_unlink(h.cid, coid, false);

      keys.insert(key);
      r = object_map->rm_keys_by_prefix(prefix, keys);   
      if (r < 0) {
	derr << __func__ << " clear wal error " << dendl;
	goto out;
      }

      r = object_map->sync();
      if (r < 0) {
	derr << __func__ << " sync wal & metadata error " << dendl;
	goto out;
      }
    } else if (h.state == compression_header_t::STATE_PROGRESS) {

      r = lfn_unlink(h.cid, oid, false);

      r = lfn_link(h.cid, h.cid, coid, h.oid);

      r = lfn_unlink(h.cid, coid, false);

      keys.insert(key);
      r = object_map->rm_keys_by_prefix(prefix, keys);   
      if (r < 0) {
	derr << __func__ << " clear wal error " << dendl;
	goto out;
      }

      r = object_map->sync();
      if (r < 0) {
	derr << __func__ << " sync wal & metadata error " << dendl;
	goto out;
      }
    }
  }
out2:
  lfn_close(cfd);
out1:
  lfn_close(fd);
out:
  dout(20) << __func__ << " r = " << r << dendl;
  return r;
}

int CStore::_wal_replay() {
  int r = 0;
  compression_header_t h(coll_t(), ghobject_t(), compression_header_t::STATE_INIT);
  bufferlist bl;
  bufferlist::iterator p;
  DBCStoreObjectMap::CStoreObjectMapIterator it;

  // replay compress wal
  dout(20) << __func__ << " replay compress wal" << dendl;
  it = object_map->get_whole_iterator(PREFIX_COMPRESS);
  for(it->lower_bound(string()); it->valid(); it->next()) {
    bl = it->value();
    p = bl.begin();
    try {
      ::decode(h, p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode compression wal " << dendl;
    }
    dout(20) << "compress object " << h.cid << "/" << h.oid << dendl;
    r = _replay_event(h.cid, h.oid, h.state, true);
    if (r < 0)
      goto out;
  }

  // replay decompress wal
  dout(20) << __func__ << " replay decompress wal" << dendl;
  it = object_map->get_whole_iterator(PREFIX_DECOMPRESS);
  for(it->lower_bound(string()); it->valid(); it->next()) {
    bl = it->value();
    p = bl.begin();
    try {
      ::decode(h, p);
    } catch (buffer::error& e) {
      derr << __func__ << " failed to decode compression wal " << dendl;
      return -EIO;
    }
    dout(20) << "decompress object " << h.cid << "/" << h.oid << dendl;
    r = _replay_event(h.cid, h.oid, h.state, false);
    if (r < 0)
      goto out;
  }
out:
  dout(20) << __func__ << " r= " << r << dendl;
  return r;
}

bool CStore::_need_compress(const coll_t &cid, const ghobject_t &oid) {

	uint32_t recency = g_conf->cstore_min_recency_for_compress;
	bool in_hit_set = false;
	{
		Mutex::Locker l(hit_set_lock);
	  in_hit_set = hit_set->contains(oid.hobj);
	}

	// persist hit set, without this code
	// object cannot be compressed if there is no request for a long period
	if (hit_set_timeout()) {
		Mutex::Locker l(hit_set_lock);
		hit_set_persist();
	}

	dout(20) << __func__ << "  " << cid << " / " << oid << " in_hit_set " << in_hit_set << dendl;
	switch(recency) {
		case 0:
			break;
		case 1:
			if (in_hit_set) {
				break;
			} else {
				return true;
			}
			break;
		default:
			{
			  unsigned count = (int)in_hit_set;
				if (count) {
					for(map<uint64_t, HitSet>::reverse_iterator it = hit_set_map.rbegin();
							it != hit_set_map.rend(); ++it) {
						bool in_hist_hit_set = false;
						{
							Mutex::Locker l(hit_set_lock);
							in_hist_hit_set = it->second.contains(oid.hobj);
						}
						if (in_hist_hit_set)
							break;
						++count;
						if (count > recency)
							break;
					}
				}
				if (count > recency)
					break;
				return true;
			}
			break;
	}
  return false;
}

void CStore::_filter_comp(coll_t &cid, ghobject_t &oid) {
  set<string> keys;
  map<string, bufferlist> values;
  objnode_t *obj = new objnode_t(oid, m_block_size, 0);

  keys.insert(OBJ_DATA);
  int r = object_map->get_xattrs(oid, keys, &values);
	if (r < 0) {
		if (r == -ENOENT) {
			dout(20) << __func__ << " object " << oid << " maybe moved or renamed " << dendl;
		} else {
      derr << __func__ << " get object metadata error " << cpp_strerror(r) << dendl;
		}
    return;
  }

  bufferlist::iterator p = values[OBJ_DATA].begin();
  ::decode(*obj, p);
  if (obj->is_compressed())
    return;

  dout(20) << __func__ << " object " << cid << "/" << oid << dendl;
  if (g_conf->cstore_inject_compress) {
    // compress object randomly
		int s;
		thread_local unsigned seed = (unsigned) time(nullptr) +
			      (unsigned) std::hash<std::thread::id>()(std::this_thread::get_id());
		s = rand_r(&seed) % 2;
		if (s == 0) {
      comp_wq.queue(obj);
		} else {
			CompContext *c = new CompContext(this, cid, oid);
      timer.add_event_after(g_conf->cstore_compress_interval, c);
		}
  } else {
    // FIXME
    // compress objects depending on hotness 
		bool need_compress = _need_compress(cid, oid);
		dout(20) << __func__ << "  " << need_compress << dendl;
    if (need_compress) {
      comp_wq.queue(obj);
      // _filter_comp is executed by timer thread, timer_lock is locked
    } else {
      // _filter_comp is executed by timer thread, timer_lock is locked
			CompContext *c = new CompContext(this, cid, oid);
      timer.add_event_after(g_conf->cstore_compress_interval, c);
    }
  }
}

int CStore::_compress(const ghobject_t &oid, ThreadPool::TPHandle &handle) {
  comp_lock.Lock();
  while(in_progress_op.count(oid)) {
    dout(20) << "object " << oid << " is in progress op, wait ..." << dendl;
    comp_cond.Wait(comp_lock);
  }

  in_progress_comp.insert(oid);
  comp_lock.Unlock();

  int r;
	coll_t cid;
  CFDRef fd, cfd;
  bufferlist bl, cbl;
	bufferlist::iterator bp;
	CompressorRef c;
	uint64_t rawlen, wantlen;
  map<uint64_t, uint64_t> exomap;
  set<string> keys;
  map<string, bufferlist> vals;
  objnode_t *node;
  string key;
  string prefix = PREFIX_COMPRESS;
  char buf[2];
  map<string, bufferptr> aset;
  struct stat st;

  handle.reset_tp_timeout();

  dout(20) << __func__ << " " << cid << "/" << oid << dendl;

  keys.insert(OBJ_DATA);
  r = object_map->get_xattrs(oid, keys, &vals);
  if (r < 0) {
    derr << "cannot get object metadata " << cpp_strerror(r) << dendl;
    goto out;
  }
  assert(r==0);
  node = new objnode_t(oid, m_block_size, 0);
  bp = vals[OBJ_DATA].begin();
  ::decode(*node, bp);
  if (node->is_compressed())
    goto out;
  keys.clear();
  vals.clear();

  // get object header to get collection
	r = object_map->get_map(oid, bl);
	if (r == -ENOENT) {
		dout(20) << __func__ << " object "  << oid << " maybe delete or move" << dendl;
		goto out;
	}
	if (r < 0) {
    derr << __func__ << " cannot get object header" << cpp_strerror(r) << dendl;
		goto out;
	} else {
    map_header_t *h = new map_header_t();
    try {
		  bp = bl.begin();
      h->decode(bp);
    } catch (buffer::error& e) {
			derr << __func__ << " failed decode map header" << dendl;
      r = -EIO;
			goto out;
    }
		cid = h->cid;
	}
	bl.clear();

  get_object_key(oid, &key);

  r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    derr << "cannot open object " << cpp_strerror(r) << dendl;
    goto out2;
  }

  memset(&st, 0, sizeof(struct stat));
  r = ::fstat(**fd, &st);
  assert(r==0);

  r = _fgetattrs(**fd, aset);
  if (r < 0) {
    derr << "cannot get extend attrs " << cpp_strerror(r) << dendl;
    goto out2;
  }

  r = chain_fgetxattr(**fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if (r < 0) {
    derr << "cannot get spill attr " << cpp_strerror(r) << dendl;
    goto out2;
  }

  r = fiemap(cid, oid, 0, st.st_size, bl);
  if (r < 0) {
		derr << "cannot get fiemap " << cpp_strerror(r) << dendl;
    goto out2;
	}

	bp = bl.begin();
	::decode(exomap, bp);
	bl.clear();

  for(map<uint64_t, uint64_t>::iterator it = exomap.begin();
    it != exomap.end(); it++) {
    int got;
    bufferptr bptr(it->second);
    got = safe_pread(**fd, bptr.c_str(), it->second, it->first);
		dout(10) << __func__ << " extent " << it->first << "~" << it->second << dendl;
    if (got < 0) {
      dout(10) << "CStore::read(" << cid << "/" << oid << ") pread error: " << cpp_strerror(got) << dendl;
      lfn_close(fd);
      return got;
    }
    bptr.set_length(it->second);
    bl.push_back(std::move(bptr));
  }

  // compress data
  c = Compressor::create(g_ceph_context, g_conf->cstore_compress_type);
  c->compress(bl, cbl);
  dout(10) << __func__ << " " << bl.length() << " -> " << cbl.length() << dendl;
  rawlen = bl.length();
  wantlen = bl.length() * g_conf->cstore_compress_ratio;
  bl.clear();
  if (cbl.length() <= wantlen) {
    dout(20) << "Start Compress " << oid << dendl;
    ghobject_t coid = oid.make_temp_obj(NS_COMPRESS);
    compression_header_t h(cid, oid, compression_header_t::STATE_INIT);

    // persist wal
    h.state = compression_header_t::STATE_INIT;
    ::encode(h, vals[key]);
    r = object_map->set_keys_by_prefix(prefix, vals);
    if (r < 0) {
      derr << __func__ << " set wal " << oid << dendl;
      goto out;
    }
    vals.clear();
    
    r = object_map->sync();
    if (r < 0) {
      derr << __func__ << " sync wal " << dendl;
      goto out;
    }

    // write data to temp object and persist
    r = lfn_open(cid, coid, true, &cfd);
    if (r < 0) {
      derr << "cannot open temp object " << cpp_strerror(r) << dendl;
      goto out;
    }

    r = cbl.write_fd(**cfd, 0);
    if (r < 0) {
      derr << __func__ << " write error " << dendl;
      goto out1;
    }

    if (!strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
      r = chain_fsetxattr<true, true>(**cfd, XATTR_SPILL_OUT_NAME, XATTR_NO_SPILL_OUT,
                          sizeof(XATTR_NO_SPILL_OUT));
    } else {
      r = chain_fsetxattr<true, true>(**cfd, XATTR_SPILL_OUT_NAME, XATTR_SPILL_OUT,
                          sizeof(XATTR_SPILL_OUT));
    }

    r = _fsetattrs(**cfd, aset);
    if (r < 0) {
      derr << __func__ << " _fsetattrs error " << dendl;
      goto out1;
    }

    r = ::fsync(**cfd);
    if (r < 0) {
      derr << __func__ << " fsync " << coid << " error " << dendl;
      goto out1;
    }

    // update metadata and persist
    node->c_type = node->get_alg_type(g_conf->cstore_compress_type);
    ::encode(*node, vals[OBJ_DATA]);
    r = object_map->set_xattrs(oid, vals);
    if (r < 0) {
      derr << __func__ << " set metadata " << oid << dendl;
      goto out1;
    }
    vals.clear();

    h.state = compression_header_t::STATE_INIT;
    ::encode(h, vals[key]);
    r = object_map->set_keys_by_prefix(prefix, vals);
    if (r < 0) {
      derr << __func__ << " set wal " << oid << dendl;
      goto out1;
    }
    vals.clear();

    r = object_map->sync();
    if (r < 0) {
      derr << __func__ << " sync wal & metadata error " << dendl;
      goto out1;
    }

    // unlink object
    r = lfn_unlink(h.cid, oid, false);
    if (r < 0) {
      derr << __func__ << " unlink " << oid << " error " << dendl;
      goto out1;
    }

    // link object to temp object
    r = lfn_link(h.cid, h.cid, coid, h.oid);
    if (r < 0) {
      derr << __func__ << " link " << h.oid << " to " << coid << " error " << dendl;
      goto out1;
    }

    // unlink temp object
    r = lfn_unlink(h.cid, coid, false);
    if (r < 0) {
      derr << __func__ << " unlink " << coid << " error " << dendl;
      goto out1;
    }

    // clear wal and persist
    keys.insert(key);
    r = object_map->rm_keys_by_prefix(prefix, keys);   
    if (r < 0) {
      derr << __func__ << " clear wal error " << dendl;
      goto out1;
    }

    r = object_map->sync();
    if (r < 0) {
      derr << __func__ << " sync wal & metadata error " << dendl;
      goto out1;
    }
  } else {
    dout(10) << __func__ << " " << rawlen << " -> " << cbl.length() << " which is more than required " << wantlen << " leaving uncompressed " << dendl;
  }

out1:
  lfn_close(cfd);
out2:
  lfn_close(fd);
out:
  comp_lock.Lock();
  in_progress_comp.erase(oid);
  comp_lock.Unlock();
  {
    Mutex::Locker l(op_lock);
    op_cond.SignalAll();
  }
  dout(20) << __func__ << " " << cid << "/" << oid << " r=" << r << dendl;
  return r;
}

int CStore::_decompress(const coll_t& cid, const ghobject_t& oid,
  ObjnodeRef obj) {
  int r;
  CFDRef fd, cfd;
  bufferlist bl, cbl, ubl;
  map<uint64_t, uint64_t> exomap;
  set<string> keys;
  map<string, bufferlist> vals;
  string key;
  string prefix = PREFIX_DECOMPRESS;
  char buf[2];
  map<string, bufferptr> aset;
	bufferlist::iterator bp;

  dout(20) << __func__ << " " << cid << "/" << oid << dendl;
  if (!obj->is_compressed())
    return 0;

  get_object_key(oid, &key);

  r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    derr << "cannot open object " << cpp_strerror(r) << dendl;
    return r;
  }

  struct stat st;
  memset(&st, 0, sizeof(struct stat));
  r = ::fstat(**fd, &st);
  assert(r==0);

  int got;
  bufferptr bpr(st.st_size);
  got = safe_pread(**fd, bpr.c_str(), st.st_size, 0);
  if (got < 0) {
    dout(10) << "CStore::read(" << cid << "/" << oid << ") pread error: " << cpp_strerror(got) << dendl;
    lfn_close(fd);
    return got;
  }
  bpr.set_length(st.st_size);
  cbl.push_back(std::move(bpr));

	r = fiemap(cid, oid, 0, obj->size, bl);
	if (r < 0) {
		derr << "cannot get fiemap " << dendl;
		return r;
	}

	bp = bl.begin();
	::decode(exomap, bp);
	bl.clear();

  // decompress data
  CompressorRef c = Compressor::create(g_ceph_context, g_conf->cstore_compress_type);
  c->decompress(cbl, ubl);
  dout(20) << "compressed buffer len " << cbl.length() << " to " << " raw buffer " << ubl.length() << dendl;

	bp = ubl.begin();
  r = _fgetattrs(**fd, aset);
  if (r < 0) {
    derr << "cannot get extend attrs " << cpp_strerror(r) << dendl;
    return r;
  }

  r = chain_fgetxattr(**fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if (r < 0) {
    derr << "cannot get spill attr " << cpp_strerror(r) << dendl;
    return r;
  }
  lfn_close(fd);

  ghobject_t coid = oid.make_temp_obj(NS_DECOMPRESS);
  compression_header_t h(cid, oid, compression_header_t::STATE_INIT);

  // persist wal
  h.state = compression_header_t::STATE_INIT;
  ::encode(h, vals[key]);
  r = object_map->set_keys_by_prefix(prefix, vals);
  if (r < 0) {
    derr << __func__ << " set wal " << oid << dendl;
    goto out;
  }
  vals.clear();
  
  r = object_map->sync();
  if (r < 0) {
    derr << __func__ << " sync wal " << dendl;
    goto out;
  }

  // write data to temp object and persist
  r = lfn_open(cid, coid, true, &cfd);
  if (r < 0) {
    derr << "cannot open temp object " << cpp_strerror(r) << dendl;
    return r;
  }

	for(map<uint64_t, uint64_t>::iterator it = exomap.begin();
		it != exomap.end(); ++it) {
		dout(30) << __func__ << " extent " << it->first << "~" << it->second << dendl;
		bufferlist tbl;
		bp.copy(it->second, tbl);
		assert(tbl.length() == it->second);
		r = tbl.write_fd(**cfd, it->first);
		if (r < 0) {
			derr << __func__ << " write " << it->first << "~" << it->second << " error "<< dendl;
			goto out1;
		}
	}

  if (!strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
    r = chain_fsetxattr<true, true>(**cfd, XATTR_SPILL_OUT_NAME, XATTR_NO_SPILL_OUT,
			sizeof(XATTR_NO_SPILL_OUT));
  } else {
    r = chain_fsetxattr<true, true>(**cfd, XATTR_SPILL_OUT_NAME, XATTR_SPILL_OUT,
			sizeof(XATTR_SPILL_OUT));
  }

  r = _fsetattrs(**cfd, aset);
  if (r < 0) {
    derr << __func__ << " _fsetattrs error " << dendl;
    goto out1;
  }

  r = ::fsync(**cfd);
  if (r < 0) {
    derr << __func__ << " fsync " << coid << " error " << dendl;
    goto out;
  }

  lfn_close(fd);

  // update metadata and persist
  obj->c_type = objnode_t::COMP_ALG_NONE;
  ::encode(*obj, vals[OBJ_DATA]);
  r = object_map->set_xattrs(oid, vals);
  if (r < 0) {
    derr << __func__ << " set metadata " << oid << dendl;
    goto out;
  }
  vals.clear();

  h.state = compression_header_t::STATE_PROGRESS;
  ::encode(h, vals[key]);
  r = object_map->set_keys_by_prefix(prefix, vals);
  if (r < 0) {
    derr << __func__ << " set wal " << oid << dendl;
    goto out;
  }
  vals.clear();

  r = object_map->sync();
  if (r < 0) {
    derr << __func__ << " sync wal & metadata error " << dendl;
    goto out;
  }

  // unlink object
  r = lfn_unlink(h.cid, oid, false);
  if (r < 0) {
    derr << __func__ << " unlink " << oid << " error " << dendl;
    goto out;
  }

  // link object to temp object
  r = lfn_link(h.cid, h.cid, coid, h.oid);
  if (r < 0) {
    derr << __func__ << " link " << h.oid << " to " << coid << " error " << dendl;
    goto out;
  }

  // unlink temp object
  r = lfn_unlink(h.cid, coid, false);
  if (r < 0) {
    derr << __func__ << " unlink " << coid << " error " << dendl;
    goto out;
  }

  // clear wal and persist
  keys.insert(key);
  r = object_map->rm_keys_by_prefix(prefix, keys);   
  if (r < 0) {
    derr << __func__ << " clear wal error " << dendl;
    goto out;
  }

  r = object_map->sync();
  if (r < 0) {
    derr << __func__ << " sync wal & metadata error " << dendl;
    goto out;
  }

  {
		CompContext *c = new CompContext(this, cid, oid);
		{
			Mutex::Locker l(timer_lock);
		  timer.add_event_after(g_conf->cstore_compress_interval, c);
		}
  }

out1:
  lfn_close(cfd);
out:
  dout(20) << __func__ << " " << cid << "/" << oid << " r=" << r << dendl;
  return r;
}

int CStore::_read_data(
  CFDRef fd,
  uint64_t offset,
  size_t len,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio) {

  dout(10) << __func__ << "  " << fd << "  " << offset << "~" << len << dendl;
  int got;
#ifdef HAVE_POSIX_FADVISE
  if (op_flags & CEPH_OSD_OP_FLAG_FADVISE_RANDOM)
    posix_fadvise(**fd, offset, len, POSIX_FADV_RANDOM);
  if (op_flags & CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL)
    posix_fadvise(**fd, offset, len, POSIX_FADV_SEQUENTIAL);
#endif

  bufferptr bptr(len);  // prealloc space for entire read
  got = safe_pread(**fd, bptr.c_str(), len, offset);
  if (got < 0) {
    dout(10) << "CStore::read " << fd << " pread error: " << cpp_strerror(got) << dendl;
    lfn_close(fd);
    if (!(allow_eio || !m_filestore_fail_eio || got != -EIO)) {
      derr << "CStore::read " << fd << " pread error: " << cpp_strerror(got) << dendl;
      assert(0 == "eio on pread");
    }
    return got;
  }
  dout(10) << "Read FD " << **fd << " got " << got << dendl;
  bptr.set_length(got);   // properly size the buffer
  bl.clear();
  bl.push_back(std::move(bptr));   // put it in the target bufferlist

#ifdef HAVE_POSIX_FADVISE
  if (op_flags & CEPH_OSD_OP_FLAG_FADVISE_DONTNEED)
    posix_fadvise(**fd, offset, len, POSIX_FADV_DONTNEED);
  if (op_flags & (CEPH_OSD_OP_FLAG_FADVISE_RANDOM | CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL))
    posix_fadvise(**fd, offset, len, POSIX_FADV_NORMAL);
#endif

  dout(10) << __func__ << "  " << fd << "  " << offset << "~" << len << " r=" << got << dendl;
  return got;
}

int CStore::read(
  const coll_t& _cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl,
  uint32_t op_flags,
  bool allow_eio)
{
  tracepoint(objectstore, read_enter, _cid.c_str(), offset, len);
  const coll_t& cid = !_need_temp_object_collection(_cid, oid) ? _cid : _cid.get_temp();

  dout(15) << "read " << cid << "/" << oid << " " << offset << "~" << len << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  int got = 0;
  CFDRef fd;
  set<string> keys;
  map<string, bufferlist> values;
  ObjnodeRef obj_node;
  objnode_t *node = new objnode_t(oid, m_block_size, 0);
  keys.insert(OBJ_DATA);
  int r = object_map->get_xattrs(oid, keys, &values);
  if (r < 0) {
		got = r;
    goto out;
  } else {
    bufferlist::iterator p = values[OBJ_DATA].begin();
    ::decode(*node, p);
    obj_node.reset(node);
  }

  if (obj_node->is_compressed()) {
    r = _decompress(cid, oid, obj_node);
    if (r < 0) {
      derr << "uncompresse error " << cpp_strerror(r) << dendl;
			got = r;
      goto out;
    }
  }

  r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    dout(10) << "CStore::read(" << cid << "/" << oid << ") open error: "
	     << cpp_strerror(r) << dendl;
		got = r;
		goto out;
  }

  if (offset == 0 && len == 0) {
    len = obj_node->size;
  }

  dout(10) << "CStore::read " << cid << "/" << oid << " " << offset << "~"
             << len << " from raw data" << dendl;
  got = _read_data(fd, offset, len, bl, op_flags, allow_eio);

  lfn_close(fd);

out:
  dout(10) << "CStore::read " << cid << "/" << oid << " " << offset << "~"
	   << got << "/" << len << dendl;
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  if (g_conf->filestore_debug_inject_read_err &&
      debug_data_eio(oid)) {
    return -EIO;
  } else {
    tracepoint(objectstore, read_exit, got);
    return got;
  }
}

int CStore::_do_fiemap(const coll_t& _cid, const ghobject_t& oid,
                        uint64_t offset, size_t len,
			map<uint64_t, uint64_t>& exomap) {
  dout(20) << __func__ << "  " << _cid << "/" << oid << "  " << offset << "~" << len << dendl;

  set<string> keys;
  map<string, bufferlist> values;
  objnode_t *node = new objnode_t(oid, m_block_size, 0);

  keys.insert(OBJ_DATA);
  int r = object_map->get_xattrs(oid, keys, &values);
  if (r < 0) {
    dout(0) << "cannot get object data" << dendl;
    return r;
  }

  bufferlist::iterator p = values[OBJ_DATA].begin();
  ::decode(*node, p);
  uint64_t s = offset / m_block_size;
  uint64_t e = P2ROUNDUP(offset+len, m_block_size) / m_block_size;
  uint64_t n;
  while((node->get_next_set_block(s, &n) != -1) && (s < e)) {
		dout(30) << __func__ << " get next " << n << " from " << s << dendl;
    if (exomap.empty()) {
      if (offset > n * m_block_size) {
	      exomap[offset] = m_block_size * (n + 1) - offset;
			} else {
        exomap[n * m_block_size] = m_block_size;
			}
      s = n+1;
      continue;
    }
    if ((exomap.rbegin()->first +  exomap.rbegin()->second) == n * m_block_size) {
      exomap.rbegin()->second = m_block_size + exomap.rbegin()->second;
    } else {
      if (offset > n * m_block_size)
	      exomap[offset] = m_block_size * (n + 1) - offset;
      else
        exomap[n * m_block_size] = m_block_size;
    }
    s = n+1;
  }
  map<uint64_t, uint64_t>::reverse_iterator rb = exomap.rbegin();
  if (rb->first + rb->second > offset + len)
    rb->second = offset + len - rb->first;
  return r;
}

int CStore::fiemap(const coll_t& _cid, const ghobject_t& oid,
                    uint64_t offset, size_t len,
                    bufferlist& bl)
{
  tracepoint(objectstore, fiemap_enter, _cid.c_str(), offset, len);
  const coll_t& cid = !_need_temp_object_collection(_cid, oid) ? _cid : _cid.get_temp();
  map<uint64_t, uint64_t> exomap;

  if ((!backend->has_seek_data_hole() && !backend->has_fiemap()) ||
      len <= (size_t)m_filestore_fiemap_threshold) {
    map<uint64_t, uint64_t> m;
    m[offset] = len;
    ::encode(m, bl);
    return 0;
  }

  dout(15) << "fiemap " << cid << "/" << oid << " " << offset << "~" << len << dendl;

  int r = _do_fiemap(_cid, oid, offset, len, exomap);


  if (r >=0 )
    ::encode(exomap, bl);
  dout(10) << "fiemap " << cid << "/" << oid << " " << offset << "~" << len << " = " << r << " num_extents=" << exomap.size() << " " << exomap << dendl;
  tracepoint(objectstore, fiemap_exit, r);
  return r;
}


int CStore::_remove(const coll_t& cid, const ghobject_t& oid,
		       const CStoreSequencerPosition &spos)
{
  dout(15) << "remove " << cid << "/" << oid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  int r = lfn_unlink(cid, oid, spos);
	if (r < 0) {
		derr << __func__ << " unlink error " << cpp_strerror(r) << dendl;
		goto out;
	}

	r = object_map->remove_map(oid);
	if (r < 0) {
		derr << __func__ << " remove map header error " << cpp_strerror(r) << dendl;
		goto out;
	}
  
out:
  dout(10) << "remove " << cid << "/" << oid << " = " << r << dendl;
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}

int CStore::_truncate(const coll_t& cid, const ghobject_t& oid, uint64_t size)
{
  dout(15) << "truncate " << cid << "/" << oid << " size " << size << dendl;
  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  CFDRef fd;
  set<string> keys;
  map<string, bufferlist> values;
  ObjnodeRef obj;
	CompContext *c = NULL;
  objnode_t *node = new objnode_t(oid, m_block_size, 0);

  keys.insert(OBJ_DATA);
  int r = object_map->get_xattrs(oid, keys, &values);
  if (r < 0) {
    goto out;
  } else {
	  bufferlist::iterator p = values[OBJ_DATA].begin();
	  ::decode(*node, p);
		obj.reset(node);
	}

  if (obj->is_compressed()) {
    r = _decompress(cid, oid, obj);
    if (r < 0) {
      derr << __func__ << " uncompresse error " << cpp_strerror(r) << dendl;
      goto out;
    }
  }

  r = lfn_open(cid, oid, false, &fd);
  if (r < 0)
    goto out;

  r = ::ftruncate(**fd, size);
  if (r < 0) {
    derr << __func__ << " ftruncate " << cpp_strerror(r) << dendl;
    goto out1;
  }

  obj->set_size(size);
  obj->update_blocks(0, size);

  values[OBJ_DATA].clear();
  ::encode(*obj, values[OBJ_DATA]);
  r = object_map->set_xattrs(oid, values, NULL);
  if (r < 0) {
    derr << "update object data r=" << cpp_strerror(r) << dendl;
    goto out1;
  }

	c = new CompContext(this, cid, oid);
	{
		Mutex::Locker l(timer_lock);
		timer.add_event_after(g_conf->cstore_compress_interval, c);
	}

out1:
  lfn_close(fd);
out:
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  dout(10) << "truncate " << cid << "/" << oid << " size " << size << " = " << r << dendl;

  return r;
}

int CStore::_touch(const coll_t& cid, const ghobject_t& oid)
{
  dout(15) << "touch " << cid << "/" << oid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  CFDRef fd;
  set<string> keys;
  map<string, bufferlist> values;
	bufferlist bl;
  objnode_t *node;
	map_header_t *mh = NULL;
	bool exist = false;
	CompContext *c = NULL;

  keys.insert(OBJ_DATA);
  int r = object_map->get_xattrs(oid, keys, &values);
  if (r < 0) {
    if (r == -ENOENT) {
			dout(20) << __func__ << " not exist " << dendl;
      node = new objnode_t(oid, m_block_size, 0);
    } else {
      goto out;
    }
  } else {
		exist = true;
    node = new objnode_t(oid, m_block_size, 0);
    bufferlist::iterator p = values[OBJ_DATA].begin();
    ::decode(*node, p);
  }
  r = lfn_open(cid, oid, true, &fd);
  if (r < 0) {
    goto out;
  } else {
    lfn_close(fd);
  }

  values[OBJ_DATA].clear();
  ::encode(*node, values[OBJ_DATA]);
  r = object_map->set_xattrs(oid, values, NULL);
	if (!exist) {
	  mh = new map_header_t(cid, oid);
	  ::encode(*mh, bl);
	  r = object_map->set_map(oid, bl);
	}

	c = new CompContext(this, cid, oid);
	{
		Mutex::Locker l(timer_lock);
		timer.add_event_after(g_conf->cstore_compress_interval, c);
	}
out:
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  dout(10) << "touch " << cid << "/" << oid << " = " << r << dendl;

  return r;
}

int CStore::_write_data(CFDRef fd, uint64_t offset,
                        size_t len, const bufferlist& bl,
		        uint32_t fadvise_flags) {
  int r;
  // write
  r = bl.write_fd(**fd, offset);
  if (r == 0)
    r = bl.length();

  return r;
}

int CStore::_write(const coll_t& cid, const ghobject_t& oid,
                     uint64_t offset, size_t len,
                     const bufferlist& bl, uint32_t fadvise_flags)
{
  dout(15) << "write " << cid << "/" << oid << " " << offset << "~" << len << dendl;
  int r;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  CFDRef fd;
  set<string> keys;
  map<string, bufferlist> values;
  ObjnodeRef obj;
	bufferlist mbl;
	map_header_t *mh = NULL;
	bool exist = false;
	CompContext *c = NULL;


  objnode_t *node = new objnode_t(oid, m_block_size, offset+len);;
  keys.insert(OBJ_DATA);
  r = object_map->get_xattrs(oid, keys, &values);
  if (r < 0) {
    if (r == -ENOENT) {
			dout(20) << __func__ << " not exist " << dendl;
      obj.reset(node);
      obj->c_type = objnode_t::COMP_ALG_NONE;
    } else {
      dout(0) << __func__ << " cannot find object data " << oid << dendl;
      goto out;
    }
  } else {
		exist = true;
    bufferlist::iterator p = values[OBJ_DATA].begin();
    ::decode(*node, p);
    obj.reset(node);
  }

  if (obj->is_compressed()) {
    r = _decompress(cid, oid, obj);
    if (r < 0) {
      derr << " decompress error " << dendl;
      goto out;
    }
  } 
  // write uncompressed data
  r = lfn_open(cid, oid, true, &fd);
  if (r < 0) {
    dout(0) << "write couldn't open " << cid << "/"
	    << oid << ": "
	    << cpp_strerror(r) << dendl;
    goto out;
  }

  r = _write_data(fd, offset, len, bl, fadvise_flags);

  if (r >= 0 && m_filestore_sloppy_crc) {
    int rc = backend->_crc_update_write(**fd, offset, len, bl);
    assert(rc >= 0);
  }
 
  if (replaying || m_disable_wbthrottle) {
    if (fadvise_flags & CEPH_OSD_OP_FLAG_FADVISE_DONTNEED) {
        posix_fadvise(**fd, 0, 0, POSIX_FADV_DONTNEED);
    }
  } else {
    wbthrottle.queue_wb(fd, oid, offset, len,
        fadvise_flags & CEPH_OSD_OP_FLAG_FADVISE_DONTNEED);
  }

  // update blocks map
	assert(!obj->is_compressed());
  obj->update_blocks(offset, len);
  values[OBJ_DATA].clear();
  ::encode(*obj, values[OBJ_DATA]);

  r = object_map->set_xattrs(oid, values, NULL);
  if (r < 0 ) {
		derr << __func__ << " update metadata error " << cpp_strerror(r) << dendl;
		goto out1;
	}

	if (!exist) {
    mh = new map_header_t(cid, oid);
		::encode(*mh, mbl);
	  r = object_map->set_map(oid, mbl);
	}

	c = new CompContext(this, cid, oid);
	{
		Mutex::Locker l(timer_lock);
		timer.add_event_after(g_conf->cstore_compress_interval, c);
	}

out1:
  lfn_close(fd);
out:
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  dout(10) << "write " << cid << "/" << oid << " " << offset << "~" << len << " = " << r << dendl;

  return r;
}

int CStore::_zero(const coll_t& cid, const ghobject_t& oid, uint64_t offset, size_t len)
{
  dout(15) << "zero " << cid << "/" << oid << " " << offset << "~" << len << dendl;
  int ret = 0;

  {
    bufferlist bl;
    bl.append_zero(len);
    ret = _write(cid, oid, offset, len, bl);
  }

  dout(20) << "zero " << cid << "/" << oid << " " << offset << "~" << len << " = " << ret << dendl;
  return ret;
}

int CStore::_clone(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid,
		      const CStoreSequencerPosition& spos)
{
  dout(15) << "clone " << cid << "/" << oldoid << " -> " << cid << "/" << newoid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oldoid)) {
    dout(20) << __func__ << " object " << oldoid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oldoid);
  op_lock.Unlock();

  int r;
  CFDRef o, n;
	bufferlist bl;
	map_header_t *mh = NULL;

  if (_check_replay_guard(cid, newoid, spos) < 0) {
		r = 0;
		goto out2;
	}

  {
    CStoreIndex index;
    r = lfn_open(cid, oldoid, false, &o, &index);
    if (r < 0) {
      goto out2;
    }
    assert(NULL != (index.index));
    RWLock::WLocker l((index.index)->access_lock);

    r = lfn_open(cid, newoid, true, &n, &index);
    if (r < 0) {
      goto out;
    }
    r = ::ftruncate(**n, 0);
    if (r < 0) {
      r = -errno;
      goto out3;
    }

    r = _do_clone_full(**o, **n);
    if (r < 0) {
      goto out3;
    }

    dout(20) << "objectmap clone" << dendl;
    r = object_map->clone(oldoid, newoid, &spos);
    if (r < 0 && r != -ENOENT)
      goto out3;

		mh = new map_header_t(cid, newoid);
		::encode(*mh, bl);
		r = object_map->set_map(newoid, bl);
		if (r < 0)
			goto out3;
  }

  {
    char buf[2];
    map<string, bufferptr> aset;
    r = _fgetattrs(**o, aset);
    if (r < 0)
      goto out3;

    r = chain_fgetxattr(**o, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
    if (r >= 0 && !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
      r = chain_fsetxattr<true, true>(**n, XATTR_SPILL_OUT_NAME, XATTR_NO_SPILL_OUT,
                          sizeof(XATTR_NO_SPILL_OUT));
    } else {
      r = chain_fsetxattr<true, true>(**n, XATTR_SPILL_OUT_NAME, XATTR_SPILL_OUT,
                          sizeof(XATTR_SPILL_OUT));
    }
    if (r < 0)
      goto out3;

    r = _fsetattrs(**n, aset);
    if (r < 0)
      goto out3;
  }

  // clone is non-idempotent; record our work.
  _set_replay_guard(**n, spos, &newoid);

 out3:
  lfn_close(n);
 out:
  lfn_close(o);
 out2:
  dout(10) << "clone " << cid << "/" << oldoid << " -> " << cid << "/" << newoid << " = " << r << dendl;
  
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oldoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int CStore::_do_fiemap_full(int fd, map<uint64_t, uint64_t>& m) {
  struct fiemap *fiemap = NULL;
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    return r;
  }
  dout(10) << __func__ << " st " << st.st_size << dendl;
  r = backend->do_fiemap(fd, 0, st.st_size, &fiemap);
  dout(10) << __func__ << " fiemap r=" << r << dendl;
  if (r < 0) {
    return r;
  }

  dout(10) << __func__ << " fiemap  " << fiemap->fm_mapped_extents << dendl;
  if (fiemap->fm_mapped_extents == 0) {
    free(fiemap);
    return r;
  }

  uint64_t i = 0;
  struct fiemap_extent *extent = &fiemap->fm_extents[0];

  while (i < fiemap->fm_mapped_extents) {
    struct fiemap_extent *next = extent + 1;
    dout(10) << __func__ << "  " << fiemap->fm_mapped_extents << " fe_logical="
            << extent->fe_logical << " fe_length=" << extent->fe_length << dendl;
    while ((i < fiemap->fm_mapped_extents - 1) && 
	   (extent->fe_logical + extent->fe_length == next->fe_logical)) {
      next->fe_length += extent->fe_length;
      next->fe_logical = extent->fe_logical;
      extent = next;
      next = extent + 1;
      i++;
    }
    if (extent->fe_logical + extent->fe_length > (uint64_t)st.st_size)
      extent->fe_length = st.st_size - extent->fe_logical;
    m[extent->fe_logical] = extent->fe_length;
    i++;
    extent++;
  }
  return r;
}

int CStore::_do_seek_hole_data_full(int fd, map<uint64_t, uint64_t>& m) {

  uint64_t offset = 0;
  uint64_t len;
  struct stat st;
  int r = ::fstat(fd, &st);
  if (r < 0) {
    return r;
  }
  len = st.st_size;

#if defined(__linux__) && defined(SEEK_HOLE) && defined(SEEK_DATA)
  off_t hole_pos, data_pos;

  // If lseek fails with errno setting to be ENXIO, this means the current
  // file offset is beyond the end of the file.
  off_t start = offset;
  while(start < (off_t)(offset + len)) {
    data_pos = lseek(fd, start, SEEK_DATA);
    if (data_pos < 0) {
      if (errno == ENXIO)
        break;
      else {
        r = -errno;
        dout(10) << "failed to lseek: " << cpp_strerror(r) << dendl;
	return r;
      }
    } else if (data_pos > (off_t)(offset + len)) {
      break;
    }

    hole_pos = lseek(fd, data_pos, SEEK_HOLE);
    if (hole_pos < 0) {
      if (errno == ENXIO) {
        break;
      } else {
        r = -errno;
        dout(10) << "failed to lseek: " << cpp_strerror(r) << dendl;
	return r;
      }
    }

    if (hole_pos >= (off_t)(offset + len)) {
      m[data_pos] = offset + len - data_pos;
      break;
    }
    m[data_pos] = hole_pos - data_pos;
    start = hole_pos;
  }

  return r;
#else
  m[offset] = len;
  return 0;
#endif
}

int CStore::_do_clone_full(int from, int to) {
  return backend->clone_full(from, to);
}

int CStore::_do_sparse_copy_full(int from, int to) {
  struct stat st;
  int r = ::fstat(from, &st);
  if (r < 0)
    return r;

  if (st.st_size == 0)
    return 0;

  dout(10) << __func__ << "  from " << from  << " to " << to << " with size " << st.st_size << dendl;
  map<uint64_t, uint64_t> exomap;
  if (backend->has_seek_data_hole()) {
    dout(15) << "seek hole/data " << dendl;
    r = _do_seek_hole_data_full(from, exomap);
  } else if (backend->has_fiemap()) {
    dout(15) << "fiemap ioctl" << dendl;
    r = _do_fiemap_full(from, exomap);
  }

  uint64_t written = 0;
  if (r < 0)
    goto out;

  for(map<uint64_t, uint64_t>::iterator it = exomap.begin(); it != exomap.end(); ++it) {
		dout(20) << __func__ << "  " << it->first << it->second << dendl; 
    r = _do_copy_range(from, to, it->first, it->second, it->first);
    if (r < 0) {
      derr << "CStore::_do_copy_range err at " << it->first << "~" << it->second << cpp_strerror(r) << dendl;
      break;
    }
    written += it->second;
  }
  if (r >= 0) {
    struct stat dst;
    r = ::fstat(to, &dst);
    if (r < 0) {
      r = -errno;
      derr << __func__ << ": fstat error at " << to << " " << cpp_strerror(r) << dendl;
       goto out;
    }
    if (dst.st_size < (int)(st.st_size)) {
      r = ::ftruncate(to, st.st_size);
      if (r < 0) {
        r = -errno;
        derr << __func__ << ": ftruncate error at " << st.st_size << " " << cpp_strerror(r) << dendl;
        goto out;
      }
    }
    r = written;
  }
 out:
  dout(10) << __func__ << "  from " << from  << " to " << to  << " r=" << r << dendl;
  return r;
}

int CStore::_do_copy_full(int from, int to, bool skip_sloppycrc) {
  dout(10) << __func__ << " " << from << " to " << to << dendl;
  struct stat st;
  int r = ::fstat(from, &st);
  if (r < 0)
    return r;

  r = _do_copy_range(from, to, 0, st.st_size, 0);
  dout(10) << __func__ << " " << from << " to " << to << " r=" << r << dendl;
  return r;
}

int CStore::_do_copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff) {
  dout(20) << "_do_copy_range " << srcoff << "~" << len << " to " << dstoff << dendl;
  int r = 0;
  loff_t pos = srcoff;
  loff_t end = srcoff + len;
  int buflen = 4096 * 16; //limit by pipe max size.see fcntl

#ifdef CEPH_HAVE_SPLICE
  if (backend->has_splice()) {
    int pipefd[2];
    if (pipe(pipefd) < 0) {
      r = -errno;
      derr << " pipe " << " got " << cpp_strerror(r) << dendl;
      return r;
    }

    loff_t dstpos = dstoff;
    while (pos < end) {
      int l = MIN(end-pos, buflen);
      r = safe_splice(from, &pos, pipefd[1], NULL, l, SPLICE_F_NONBLOCK);
      dout(10) << "  safe_splice read from " << pos << "~" << l << " got " << r << dendl;
      if (r < 0) {
	derr << "CStore::_do_copy_range: safe_splice read error at " << pos << "~" << len
	  << ", " << cpp_strerror(r) << dendl;
	break;
      }
      if (r == 0) {
	// hrm, bad source range, wtf.
	r = -ERANGE;
	derr << "CStore::_do_copy_range got short read result at " << pos
	  << " of fd " << from << " len " << len << dendl;
	break;
      }

      r = safe_splice(pipefd[0], NULL, to, &dstpos, r, 0);
      dout(10) << " safe_splice write to " << to << " len " << r
	<< " got " << r << dendl;
      if (r < 0) {
	derr << "CStore::_do_copy_range: write error at " << pos << "~"
	  << r << ", " << cpp_strerror(r) << dendl;
	break;
      }
    }
    close(pipefd[0]);
    close(pipefd[1]);
  } else
#endif
  {
    int64_t actual;

    actual = ::lseek64(from, srcoff, SEEK_SET);
    if (actual != (int64_t)srcoff) {
      if (actual < 0)
        r = -errno;
      else
        r = -EINVAL;
      derr << "lseek64 to " << srcoff << " got " << cpp_strerror(r) << dendl;
      return r;
    }
    actual = ::lseek64(to, dstoff, SEEK_SET);
    if (actual != (int64_t)dstoff) {
      if (actual < 0)
        r = -errno;
      else
        r = -EINVAL;
      derr << "lseek64 to " << dstoff << " got " << cpp_strerror(r) << dendl;
      return r;
    }

    char buf[buflen];
    while (pos < end) {
      int l = MIN(end-pos, buflen);
      r = ::read(from, buf, l);
      dout(25) << "  read from " << pos << "~" << l << " got " << r << dendl;
      if (r < 0) {
	if (errno == EINTR) {
	  continue;
	} else {
	  r = -errno;
	  derr << "FileStore::_do_copy_range: read error at " << pos << "~" << len
	    << ", " << cpp_strerror(r) << dendl;
	  break;
	}
      }
      if (r == 0) {
	// hrm, bad source range, wtf.
	r = -ERANGE;
	derr << "FileStore::_do_copy_range got short read result at " << pos
	  << " of fd " << from << " len " << len << dendl;
	break;
      }
      int op = 0;
      while (op < r) {
	int r2 = safe_write(to, buf+op, r-op);
	dout(25) << " write to " << to << " len " << (r-op)
	  << " got " << r2 << dendl;
	if (r2 < 0) {
	  r = r2;
	  derr << "FileStore::_do_copy_range: write error at " << pos << "~"
	    << r-op << ", " << cpp_strerror(r) << dendl;

	  break;
	}
	op += (r-op);
      }
      if (r < 0)
	break;
      pos += r;
    }
  }
  assert(pos == end);
  dout(20) << "_do_copy_range " << srcoff << "~" << len << " to " << dstoff << " = " << r << dendl;
  return r;
}

int CStore::_clone_range(const coll_t& cid, const ghobject_t& oldoid, const ghobject_t& newoid,
			    uint64_t srcoff, uint64_t len, uint64_t dstoff,
			    const CStoreSequencerPosition& spos)
{
  dout(15) << "clone_range " << cid << "/" << oldoid << " -> " << cid << "/" << newoid << " " << srcoff << "~" << len << " to " << dstoff << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oldoid)) {
    dout(20) << __func__ << " object " << oldoid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oldoid);
  op_lock.Unlock();

  int r;
  CFDRef o, n;
  bufferlist bl;
  if (_check_replay_guard(cid, newoid, spos) < 0) {
		r = 0;
		goto out2;
	}

  r = lfn_open(cid, oldoid, false, &o);
  if (r < 0) {
    goto out2;
  }
  r = lfn_open(cid, newoid, true, &n);
  if (r < 0) {
    goto out1;
  }

  r = read(cid, oldoid, srcoff, len, bl, 0, false);
  if (r < 0) {
    goto out;
  }

  r = _write(cid, newoid, dstoff, len, bl, 0);
  if (r < 0) {
    goto out;
  }

  // clone is non-idempotent; record our work.
  _set_replay_guard(**n, spos, &newoid);

 out:
  lfn_close(n);
 out1:
  lfn_close(o);
 out2:
  dout(10) << "clone_range " << cid << "/" << oldoid << " -> " << cid << "/" << newoid << " "
	   << srcoff << "~" << len << " to " << dstoff << " = " << r << dendl;
  
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oldoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}

class CStoreSyncEntryTimeout : public Context {
public:
  explicit CStoreSyncEntryTimeout(int commit_timeo)
    : m_commit_timeo(commit_timeo)
  {
  }

  void finish(int r) {
    BackTrace *bt = new BackTrace(1);
    generic_dout(-1) << "CStore: sync_entry timed out after "
	   << m_commit_timeo << " seconds.\n";
    bt->print(*_dout);
    *_dout << dendl;
    delete bt;
    ceph_abort();
  }
private:
  int m_commit_timeo;
};

void CStore::sync_entry()
{
  lock.Lock();
  while (!stop) {
    utime_t max_interval;
    max_interval.set_from_double(m_filestore_max_sync_interval);
    utime_t min_interval;
    min_interval.set_from_double(m_filestore_min_sync_interval);

    utime_t startwait = ceph_clock_now(g_ceph_context);
    if (!force_sync) {
      dout(20) << "sync_entry waiting for max_interval " << max_interval << dendl;
      sync_cond.WaitInterval(g_ceph_context, lock, max_interval);
    } else {
      dout(20) << "sync_entry not waiting, force_sync set" << dendl;
    }

    if (force_sync) {
      dout(20) << "sync_entry force_sync set" << dendl;
      force_sync = false;
    } else if (stop) {
      dout(20) << __func__ << " stop set" << dendl;
      break;
    } else {
      // wait for at least the min interval
      utime_t woke = ceph_clock_now(g_ceph_context);
      woke -= startwait;
      dout(20) << "sync_entry woke after " << woke << dendl;
      if (woke < min_interval) {
	utime_t t = min_interval;
	t -= woke;
	dout(20) << "sync_entry waiting for another " << t
		 << " to reach min interval " << min_interval << dendl;
	sync_cond.WaitInterval(g_ceph_context, lock, t);
      }
    }

    list<Context*> fin;
  again:
    fin.swap(sync_waiters);
    lock.Unlock();

    op_tp.pause();
    comp_tp.pause();
    if (apply_manager.commit_start()) {
      utime_t start = ceph_clock_now(g_ceph_context);
      uint64_t cp = apply_manager.get_committing_seq();

      timer_lock.Lock();
      CStoreSyncEntryTimeout *sync_entry_timeo =
	new CStoreSyncEntryTimeout(m_filestore_commit_timeout);
      timer.add_event_after(m_filestore_commit_timeout, sync_entry_timeo);
      timer_lock.Unlock();

      logger->set(l_os_committing, 1);

      dout(15) << "sync_entry committing " << cp << dendl;
      stringstream errstream;
      if (g_conf->filestore_debug_omap_check && !object_map->check(errstream)) {
	derr << errstream.str() << dendl;
	assert(0);
      }

      if (backend->can_checkpoint()) {
	int err = write_op_seq(op_fd, cp);
	if (err < 0) {
	  derr << "Error during write_op_seq: " << cpp_strerror(err) << dendl;
	  assert(0 == "error during write_op_seq");
	}

	char s[NAME_MAX];
	snprintf(s, sizeof(s), COMMIT_SNAP_ITEM, (long long unsigned)cp);
	uint64_t cid = 0;
	err = backend->create_checkpoint(s, &cid);
	if (err < 0) {
	    int err = errno;
	    derr << "snap create '" << s << "' got error " << err << dendl;
	    assert(err == 0);
	}

	snaps.push_back(cp);
	apply_manager.commit_started();
	op_tp.unpause();
	comp_tp.unpause();

	if (cid > 0) {
	  dout(20) << " waiting for checkpoint " << cid << " to complete" << dendl;
	  err = backend->sync_checkpoint(cid);
	  if (err < 0) {
	    derr << "ioctl WAIT_SYNC got " << cpp_strerror(err) << dendl;
	    assert(0 == "wait_sync got error");
	  }
	  dout(20) << " done waiting for checkpoint" << cid << " to complete" << dendl;
	}
      } else
      {
	apply_manager.commit_started();
	op_tp.unpause();
	comp_tp.unpause();

	int err = object_map->sync();
	if (err < 0) {
	  derr << "object_map sync got " << cpp_strerror(err) << dendl;
	  assert(0 == "object_map sync returned error");
	}

	err = backend->syncfs();
	if (err < 0) {
	  derr << "syncfs got " << cpp_strerror(err) << dendl;
	  assert(0 == "syncfs returned error");
	}

	err = write_op_seq(op_fd, cp);
	if (err < 0) {
	  derr << "Error during write_op_seq: " << cpp_strerror(err) << dendl;
	  assert(0 == "error during write_op_seq");
	}
	err = ::fsync(op_fd);
	if (err < 0) {
	  derr << "Error during fsync of op_seq: " << cpp_strerror(err) << dendl;
	  assert(0 == "error during fsync of op_seq");
	}
      }

      utime_t done = ceph_clock_now(g_ceph_context);
      utime_t lat = done - start;
      utime_t dur = done - startwait;
      dout(10) << "sync_entry commit took " << lat << ", interval was " << dur << dendl;

      logger->inc(l_os_commit);
      logger->tinc(l_os_commit_lat, lat);
      logger->tinc(l_os_commit_len, dur);

      apply_manager.commit_finish();
      if (!m_disable_wbthrottle) {
        wbthrottle.clear();
      }

      logger->set(l_os_committing, 0);

      // remove old snaps?
      if (backend->can_checkpoint()) {
	char s[NAME_MAX];
	while (snaps.size() > 2) {
	  snprintf(s, sizeof(s), COMMIT_SNAP_ITEM, (long long unsigned)snaps.front());
	  snaps.pop_front();
	  dout(10) << "removing snap '" << s << "'" << dendl;
	  int r = backend->destroy_checkpoint(s);
	  if (r) {
	    int err = errno;
	    derr << "unable to destroy snap '" << s << "' got " << cpp_strerror(err) << dendl;
	  }
	}
      }

      dout(15) << "sync_entry committed to op_seq " << cp << dendl;

      timer_lock.Lock();
      timer.cancel_event(sync_entry_timeo);
			// cancel event of compress
      timer_lock.Unlock();
    } else {
      op_tp.unpause();
      comp_tp.unpause();
    }

    lock.Lock();
    finish_contexts(g_ceph_context, fin, 0);
    fin.clear();
    if (!sync_waiters.empty()) {
      dout(10) << "sync_entry more waiters, committing again" << dendl;
      goto again;
    }
    if (!stop && journal && journal->should_commit_now()) {
      dout(10) << "sync_entry journal says we should commit again (probably is/was full)" << dendl;
      goto again;
    }
  }
  stop = false;
  lock.Unlock();
}

void CStore::_start_sync()
{
  if (!journal) {  // don't do a big sync if the journal is on
    dout(10) << "start_sync" << dendl;
    sync_cond.Signal();
  } else {
    dout(10) << "start_sync - NOOP (journal is on)" << dendl;
  }
}

void CStore::do_force_sync()
{
  dout(10) << __func__ << dendl;
  Mutex::Locker l(lock);
  force_sync = true;
  sync_cond.Signal();
}

void CStore::start_sync(Context *onsafe)
{
  Mutex::Locker l(lock);
  sync_waiters.push_back(onsafe);
  sync_cond.Signal();
  force_sync = true;
  dout(10) << "start_sync" << dendl;
}

void CStore::sync()
{
  Mutex l("CStore::sync");
  Cond c;
  bool done;
  C_SafeCond *fin = new C_SafeCond(&l, &c, &done);

  start_sync(fin);

  l.Lock();
  while (!done) {
    dout(10) << "sync waiting" << dendl;
    c.Wait(l);
  }
  l.Unlock();
  dout(10) << "sync done" << dendl;
}

void CStore::_flush_op_queue()
{
  dout(10) << "_flush_op_queue draining op tp" << dendl;
  op_wq.drain();
  dout(10) << "_flush_op_queue waiting for apply finisher" << dendl;
  for (vector<Finisher*>::iterator it = apply_finishers.begin(); it != apply_finishers.end(); ++it) {
    (*it)->wait_for_empty();
  }
}

/*
 * flush - make every queued write readable
 */
void CStore::flush()
{
  dout(10) << "flush" << dendl;

  if (g_conf->filestore_blackhole) {
    // wait forever
    Mutex lock("CStore::flush::lock");
    Cond cond;
    lock.Lock();
    while (true)
      cond.Wait(lock);
    assert(0);
  }

  if (m_filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    dout(10) << "flush draining ondisk finisher" << dendl;
    for (vector<Finisher*>::iterator it = ondisk_finishers.begin(); it != ondisk_finishers.end(); ++it) {
      (*it)->wait_for_empty();
    }
  }

  _flush_op_queue();
  dout(10) << "flush complete" << dendl;
}

/*
 * sync_and_flush - make every queued write readable AND committed to disk
 */
void CStore::sync_and_flush()
{
  dout(10) << "sync_and_flush" << dendl;

  if (m_filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    _flush_op_queue();
  } else {
    // includes m_filestore_journal_parallel
    _flush_op_queue();
    sync();
  }
  dout(10) << "sync_and_flush done" << dendl;
}

int CStore::flush_journal()
{
  dout(10) << __func__ << dendl;
  sync_and_flush();
  sync();
  return 0;
}

int CStore::snapshot(const string& name)
{
  dout(10) << "snapshot " << name << dendl;
  sync_and_flush();

  if (!backend->can_checkpoint()) {
    dout(0) << "snapshot " << name << " failed, not supported" << dendl;
    return -EOPNOTSUPP;
  }

  char s[NAME_MAX];
  snprintf(s, sizeof(s), CLUSTER_SNAP_ITEM, name.c_str());

  int r = backend->create_checkpoint(s, NULL);
  if (r) {
    derr << "snapshot " << name << " failed: " << cpp_strerror(r) << dendl;
  }

  return r;
}

// -------------------------------
// attributes

int CStore::_fgetattr(int fd, const char *name, bufferptr& bp)
{
  char val[CHAIN_XATTR_MAX_BLOCK_LEN];
  int l = chain_fgetxattr(fd, name, val, sizeof(val));
  if (l >= 0) {
    bp = buffer::create(l);
    memcpy(bp.c_str(), val, l);
  } else if (l == -ERANGE) {
    l = chain_fgetxattr(fd, name, 0, 0);
    if (l > 0) {
      bp = buffer::create(l);
      l = chain_fgetxattr(fd, name, bp.c_str(), l);
    }
  }
  assert(!m_filestore_fail_eio || l != -EIO);
  return l;
}

int CStore::_fgetattrs(int fd, map<string,bufferptr>& aset)
{
  // get attr list
  char names1[100];
  int len = chain_flistxattr(fd, names1, sizeof(names1)-1);
  char *names2 = 0;
  char *name = 0;
  if (len == -ERANGE) {
    len = chain_flistxattr(fd, 0, 0);
    if (len < 0) {
      assert(!m_filestore_fail_eio || len != -EIO);
      return len;
    }
    dout(10) << " -ERANGE, len is " << len << dendl;
    names2 = new char[len+1];
    len = chain_flistxattr(fd, names2, len);
    dout(10) << " -ERANGE, got " << len << dendl;
    if (len < 0) {
      assert(!m_filestore_fail_eio || len != -EIO);
      delete[] names2;
      return len;
    }
    name = names2;
  } else if (len < 0) {
    assert(!m_filestore_fail_eio || len != -EIO);
    return len;
  } else {
    name = names1;
  }
  name[len] = 0;

  char *end = name + len;
  while (name < end) {
    char *attrname = name;
    if (cparse_attrname(&name)) {
      if (*name) {
        dout(20) << "fgetattrs " << fd << " getting '" << name << "'" << dendl;
        int r = _fgetattr(fd, attrname, aset[name]);
        if (r < 0) {
	  delete[] names2;
	  return r;
        }
      }
    }
    name += strlen(name) + 1;
  }

  delete[] names2;
  return 0;
}

int CStore::_fsetattrs(int fd, map<string, bufferptr> &aset)
{
  for (map<string, bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[CHAIN_XATTR_MAX_NAME_LEN];
    get_attrname(p->first.c_str(), n, CHAIN_XATTR_MAX_NAME_LEN);
    const char *val;
    if (p->second.length())
      val = p->second.c_str();
    else
      val = "";
    // ??? Why do we skip setting all the other attrs if one fails?
    int r = chain_fsetxattr(fd, n, val, p->second.length());
    if (r < 0) {
      derr << "CStore::_setattrs: chain_setxattr returned " << r << dendl;
      return r;
    }
  }
  return 0;
}

// debug EIO injection
void CStore::inject_data_error(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  dout(10) << __func__ << ": init error on " << oid << dendl;
  data_error_set.insert(oid);
}
void CStore::inject_mdata_error(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  dout(10) << __func__ << ": init error on " << oid << dendl;
  mdata_error_set.insert(oid);
}
void CStore::debug_obj_on_delete(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  dout(10) << __func__ << ": clear error on " << oid << dendl;
  data_error_set.erase(oid);
  mdata_error_set.erase(oid);
}
bool CStore::debug_data_eio(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  if (data_error_set.count(oid)) {
    dout(10) << __func__ << ": inject error on " << oid << dendl;
    return true;
  } else {
    return false;
  }
}
bool CStore::debug_mdata_eio(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  if (mdata_error_set.count(oid)) {
    dout(10) << __func__ << ": inject error on " << oid << dendl;
    return true;
  } else {
    return false;
  }
}


// objects

int CStore::getattr(const coll_t& _cid, const ghobject_t& oid, const char *name, bufferptr &bp)
{
  tracepoint(objectstore, getattr_enter, _cid.c_str());
  const coll_t& cid = !_need_temp_object_collection(_cid, oid) ? _cid : _cid.get_temp();
  dout(15) << "getattr " << cid << "/" << oid << " '" << name << "'" << dendl;
  CFDRef fd;
  int r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    goto out;
  }
  char n[CHAIN_XATTR_MAX_NAME_LEN];
  get_attrname(name, n, CHAIN_XATTR_MAX_NAME_LEN);
  r = _fgetattr(**fd, n, bp);
  lfn_close(fd);
  if (r == -ENODATA) {
    map<string, bufferlist> got;
    set<string> to_get;
    to_get.insert(string(name));
    CStoreIndex index;
    r = get_index(cid, &index);
    if (r < 0) {
      dout(10) << __func__ << " could not get index r = " << r << dendl;
      goto out;
    }
    r = object_map->get_xattrs(oid, to_get, &got);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " get_xattrs err r =" << r << dendl;
      goto out;
    }
    if (got.empty()) {
      dout(10) << __func__ << " got.size() is 0" << dendl;
      return -ENODATA;
    }
    bp = bufferptr(got.begin()->second.c_str(),
		   got.begin()->second.length());
    r = bp.length();
  }
 out:
  dout(10) << "getattr " << cid << "/" << oid << " '" << name << "' = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  if (g_conf->filestore_debug_inject_read_err &&
      debug_mdata_eio(oid)) {
    return -EIO;
  } else {
    tracepoint(objectstore, getattr_exit, r);
    return r < 0 ? r : 0;
  }
}

int CStore::getattrs(const coll_t& _cid, const ghobject_t& oid, map<string,bufferptr>& aset)
{
  tracepoint(objectstore, getattrs_enter, _cid.c_str());
  const coll_t& cid = !_need_temp_object_collection(_cid, oid) ? _cid : _cid.get_temp();
  set<string> omap_attrs;
  map<string, bufferlist> omap_aset;
  CStoreIndex index;
  dout(15) << "getattrs " << cid << "/" << oid << dendl;
  CFDRef fd;
  bool spill_out = true;
  char buf[2];

  int r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    goto out;
  }

  r = chain_fgetxattr(**fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if (r >= 0 && !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT)))
    spill_out = false;

  r = _fgetattrs(**fd, aset);
  lfn_close(fd);
  fd = CFDRef(); // defensive
  if (r < 0) {
    goto out;
  }

  if (!spill_out) {
    dout(10) << __func__ << " no xattr exists in object_map r = " << r << dendl;
    goto out;
  }

  r = get_index(cid, &index);
  if (r < 0) {
    dout(10) << __func__ << " could not get index r = " << r << dendl;
    goto out;
  }
  {
    r = object_map->get_all_xattrs(oid, &omap_attrs);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " could not get omap_attrs r = " << r << dendl;
      goto out;
    }

    r = object_map->get_xattrs(oid, omap_attrs, &omap_aset);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " could not get omap_attrs r = " << r << dendl;
      goto out;
    }
    if (r == -ENOENT)
      r = 0;
  }
  assert(omap_attrs.size() == omap_aset.size());
  for (map<string, bufferlist>::iterator i = omap_aset.begin();
	 i != omap_aset.end();
	 ++i) {
    string key(i->first);
    aset.insert(make_pair(key,
			    bufferptr(i->second.c_str(), i->second.length())));
  }
 out:
  dout(10) << "getattrs " << cid << "/" << oid << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);

  if (g_conf->filestore_debug_inject_read_err &&
      debug_mdata_eio(oid)) {
    return -EIO;
  } else {
    tracepoint(objectstore, getattrs_exit, r);
    return r;
  }
}

int CStore::_setattrs(const coll_t& cid, const ghobject_t& oid, map<string,bufferptr>& aset,
			 const CStoreSequencerPosition &spos)
{
  map<string, bufferlist> omap_set;
  set<string> omap_remove;
  map<string, bufferptr> inline_set;
  map<string, bufferptr> inline_to_set;
  CFDRef fd;
  int spill_out = -1;
  bool incomplete_inline = false;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  int r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    goto out;
  }

  char buf[2];
  r = chain_fgetxattr(**fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if (r >= 0 && !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT)))
    spill_out = 0;
  else
    spill_out = 1;

  r = _fgetattrs(**fd, inline_set);
  incomplete_inline = (r == -E2BIG);
  assert(!m_filestore_fail_eio || r != -EIO);
  dout(15) << "setattrs " << cid << "/" << oid
	   << (incomplete_inline ? " (incomplete_inline, forcing omap)" : "")
	   << dendl;

  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[CHAIN_XATTR_MAX_NAME_LEN];
    get_attrname(p->first.c_str(), n, CHAIN_XATTR_MAX_NAME_LEN);

    if (incomplete_inline) {
      chain_fremovexattr(**fd, n); // ignore any error
      omap_set[p->first].push_back(p->second);
      continue;
    }

    if (p->second.length() > m_filestore_max_inline_xattr_size) {
	if (inline_set.count(p->first)) {
	  inline_set.erase(p->first);
	  r = chain_fremovexattr(**fd, n);
	  if (r < 0)
	    goto out_close;
	}
	omap_set[p->first].push_back(p->second);
	continue;
    }

    if (!inline_set.count(p->first) &&
	  inline_set.size() >= m_filestore_max_inline_xattrs) {
	omap_set[p->first].push_back(p->second);
	continue;
    }
    omap_remove.insert(p->first);
    inline_set.insert(*p);

    inline_to_set.insert(*p);
  }

  if (spill_out != 1 && !omap_set.empty()) {
    chain_fsetxattr(**fd, XATTR_SPILL_OUT_NAME, XATTR_SPILL_OUT,
		    sizeof(XATTR_SPILL_OUT));
  }

  r = _fsetattrs(**fd, inline_to_set);
  if (r < 0)
    goto out_close;

  if (spill_out && !omap_remove.empty()) {
    r = object_map->remove_xattrs(oid, omap_remove, &spos);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " could not remove_xattrs r = " << r << dendl;
      assert(!m_filestore_fail_eio || r != -EIO);
      goto out_close;
    } else {
      r = 0; // don't confuse the debug output
    }
  }

  if (!omap_set.empty()) {
    r = object_map->set_xattrs(oid, omap_set, &spos);
    if (r < 0) {
      dout(10) << __func__ << " could not set_xattrs r = " << r << dendl;
      assert(!m_filestore_fail_eio || r != -EIO);
      goto out_close;
    }
  }
 out_close:
  lfn_close(fd);
 out:
  dout(10) << "setattrs " << cid << "/" << oid << " = " << r << dendl;

  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}


int CStore::_rmattr(const coll_t& cid, const ghobject_t& oid, const char *name,
		       const CStoreSequencerPosition &spos)
{
  dout(15) << "rmattr " << cid << "/" << oid << " '" << name << "'" << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  CFDRef fd;
  bool spill_out = true;
  bufferptr bp;

  int r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    goto out;
  }

  char buf[2];
  r = chain_fgetxattr(**fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if (r >= 0 && !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
    spill_out = false;
  }

  char n[CHAIN_XATTR_MAX_NAME_LEN];
  get_attrname(name, n, CHAIN_XATTR_MAX_NAME_LEN);
  r = chain_fremovexattr(**fd, n);
  if (r == -ENODATA && spill_out) {
    CStoreIndex index;
    r = get_index(cid, &index);
    if (r < 0) {
      dout(10) << __func__ << " could not get index r = " << r << dendl;
      goto out_close;
    }
    set<string> to_remove;
    to_remove.insert(string(name));
    r = object_map->remove_xattrs(oid, to_remove, &spos);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " could not remove_xattrs index r = " << r << dendl;
      assert(!m_filestore_fail_eio || r != -EIO);
      goto out_close;
    }
  }
 out_close:
  lfn_close(fd);
 out:
  dout(10) << "rmattr " << cid << "/" << oid << " '" << name << "' = " << r << dendl;
  
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}

int CStore::_rmattrs(const coll_t& cid, const ghobject_t& oid,
			const CStoreSequencerPosition &spos)
{
  dout(15) << "rmattrs " << cid << "/" << oid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock(); 

  map<string,bufferptr> aset;
  CFDRef fd;
  set<string> omap_attrs;
  CStoreIndex index;
  bool spill_out = true;

  int r = lfn_open(cid, oid, false, &fd);
  if (r < 0) {
    goto out;
  }

  char buf[2];
  r = chain_fgetxattr(**fd, XATTR_SPILL_OUT_NAME, buf, sizeof(buf));
  if (r >= 0 && !strncmp(buf, XATTR_NO_SPILL_OUT, sizeof(XATTR_NO_SPILL_OUT))) {
    spill_out = false;
  }

  r = _fgetattrs(**fd, aset);
  if (r >= 0) {
    for (map<string,bufferptr>::iterator p = aset.begin(); p != aset.end(); ++p) {
      char n[CHAIN_XATTR_MAX_NAME_LEN];
      get_attrname(p->first.c_str(), n, CHAIN_XATTR_MAX_NAME_LEN);
      r = chain_fremovexattr(**fd, n);
      if (r < 0) {
        dout(10) << __func__ << " could not remove xattr r = " << r << dendl;
	goto out_close;
      }
    }
  }

  if (!spill_out) {
    dout(10) << __func__ << " no xattr exists in object_map r = " << r << dendl;
    goto out_close;
  }

  r = get_index(cid, &index);
  if (r < 0) {
    dout(10) << __func__ << " could not get index r = " << r << dendl;
    goto out_close;
  }
  {
    r = object_map->get_all_xattrs(oid, &omap_attrs);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " could not get omap_attrs r = " << r << dendl;
      assert(!m_filestore_fail_eio || r != -EIO);
      goto out_close;
    }
    r = object_map->remove_xattrs(oid, omap_attrs, &spos);
    if (r < 0 && r != -ENOENT) {
      dout(10) << __func__ << " could not remove omap_attrs r = " << r << dendl;
      goto out_close;
    }
    if (r == -ENOENT)
      r = 0;
    chain_fsetxattr(**fd, XATTR_SPILL_OUT_NAME, XATTR_NO_SPILL_OUT,
		  sizeof(XATTR_NO_SPILL_OUT));
  }

 out_close:
  lfn_close(fd);
 out:
  dout(10) << "rmattrs " << cid << "/" << oid << " = " << r << dendl;

  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
 
    comp_cond.SignalAll();
  }

  return r;
}



// collections

int CStore::collection_getattr(const coll_t& c, const char *name,
				  void *value, size_t size)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "' len " << size << dendl;
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  r = chain_fgetxattr(fd, n, value, size);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int CStore::collection_getattr(const coll_t& c, const char *name, bufferlist& bl)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "'" << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  buffer::ptr bp;
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  r = _fgetattr(fd, n, bp);
  bl.push_back(std::move(bp));
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_getattr " << fn << " '" << name << "' = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}

int CStore::collection_getattrs(const coll_t& cid, map<string,bufferptr>& aset)
{
  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  dout(10) << "collection_getattrs " << fn << dendl;
  int r = 0;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  r = _fgetattrs(fd, aset);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_getattrs " << fn << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  return r;
}


int CStore::_collection_setattr(const coll_t& c, const char *name,
				  const void *value, size_t size)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << dendl;
  char n[PATH_MAX];
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  get_attrname(name, n, PATH_MAX);
  r = chain_fsetxattr(fd, n, value, size);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int CStore::_collection_rmattr(const coll_t& c, const char *name)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_rmattr " << fn << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  r = chain_fremovexattr(fd, n);
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_rmattr " << fn << " = " << r << dendl;
  return r;
}


int CStore::_collection_setattrs(const coll_t& cid, map<string,bufferptr>& aset)
{
  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  dout(15) << "collection_setattrs " << fn << dendl;
  int r = 0;
  int fd = ::open(fn, O_RDONLY);
  if (fd < 0) {
    r = -errno;
    goto out;
  }
  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[PATH_MAX];
    get_attrname(p->first.c_str(), n, PATH_MAX);
    r = chain_fsetxattr(fd, n, p->second.c_str(), p->second.length());
    if (r < 0)
      break;
  }
  VOID_TEMP_FAILURE_RETRY(::close(fd));
 out:
  dout(10) << "collection_setattrs " << fn << " = " << r << dendl;
  return r;
}

int CStore::_collection_remove_recursive(const coll_t &cid,
					    const CStoreSequencerPosition &spos)
{
  struct stat st;
  int r = collection_stat(cid, &st);
  if (r < 0) {
    if (r == -ENOENT)
      return 0;
    return r;
  }

  vector<ghobject_t> objects;
  ghobject_t max;
  while (!max.is_max()) {
    r = collection_list(cid, max, ghobject_t::get_max(), true,
			300, &objects, &max);
    if (r < 0)
      return r;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      assert(_check_replay_guard(cid, *i, spos));
      r = _remove(cid, *i, spos);
      if (r < 0)
	return r;
    }
    objects.clear();
  }
  return _destroy_collection(cid);
}

// --------------------------
// collections

int CStore::collection_version_current(const coll_t& c, uint32_t *version)
{
  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0)
    return r;

  assert(NULL != index.index);
  RWLock::RLocker l((index.index)->access_lock);

  *version = index->collection_version();
  if (*version == target_version)
    return 1;
  else
    return 0;
}

int CStore::list_collections(vector<coll_t>& ls)
{
  return list_collections(ls, false);
}

int CStore::list_collections(vector<coll_t>& ls, bool include_temp)
{
  tracepoint(objectstore, list_collections_enter);
  dout(10) << "list_collections" << dendl;

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/current", basedir.c_str());

  int r = 0;
  DIR *dir = ::opendir(fn);
  if (!dir) {
    r = -errno;
    derr << "tried opening directory " << fn << ": " << cpp_strerror(-r) << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }

  char buf[offsetof(struct dirent, d_name) + PATH_MAX + 1];
  struct dirent *de;
  while ((r = ::readdir_r(dir, (struct dirent *)&buf, &de)) == 0) {
    if (!de)
      break;
    if (de->d_type == DT_UNKNOWN) {
      // d_type not supported (non-ext[234], btrfs), must stat
      struct stat sb;
      char filename[PATH_MAX];
      snprintf(filename, sizeof(filename), "%s/%s", fn, de->d_name);

      r = ::stat(filename, &sb);
      if (r < 0) {
	r = -errno;
	derr << "stat on " << filename << ": " << cpp_strerror(-r) << dendl;
	assert(!m_filestore_fail_eio || r != -EIO);
	break;
      }
      if (!S_ISDIR(sb.st_mode)) {
	continue;
      }
    } else if (de->d_type != DT_DIR) {
      continue;
    }
    if (strcmp(de->d_name, "omap") == 0) {
      continue;
    }
    if (de->d_name[0] == '.' &&
	(de->d_name[1] == '\0' ||
	 (de->d_name[1] == '.' &&
	  de->d_name[2] == '\0')))
      continue;
    coll_t cid;
    if (!cid.parse(de->d_name)) {
      derr << "ignoging invalid collection '" << de->d_name << "'" << dendl;
      continue;
    }
    if (!cid.is_temp() || include_temp)
      ls.push_back(cid);
  }

  if (r > 0) {
    derr << "trying readdir_r " << fn << ": " << cpp_strerror(r) << dendl;
    r = -r;
  }

  ::closedir(dir);
  assert(!m_filestore_fail_eio || r != -EIO);
  tracepoint(objectstore, list_collections_exit, r);
  return r;
}

int CStore::collection_stat(const coll_t& c, struct stat *st)
{
  tracepoint(objectstore, collection_stat_enter, c.c_str());
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_stat " << fn << dendl;
  int r = ::stat(fn, st);
  if (r < 0)
    r = -errno;
  dout(10) << "collection_stat " << fn << " = " << r << dendl;
  assert(!m_filestore_fail_eio || r != -EIO);
  tracepoint(objectstore, collection_stat_exit, r);
  return r;
}

bool CStore::collection_exists(const coll_t& c)
{
  tracepoint(objectstore, collection_exists_enter, c.c_str());
  struct stat st;
  bool ret = collection_stat(c, &st) == 0;
  tracepoint(objectstore, collection_exists_exit, ret);
  return ret;
}

bool CStore::collection_empty(const coll_t& c)
{
  tracepoint(objectstore, collection_empty_enter, c.c_str());
  dout(15) << "collection_empty " << c << dendl;
  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0)
    return false;

  assert(NULL != index.index);
  RWLock::RLocker l((index.index)->access_lock);

  vector<ghobject_t> ls;
  r = index->collection_list_partial(ghobject_t(), ghobject_t::get_max(), true,
				     1, &ls, NULL);
  if (r < 0) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return false;
  }
  bool ret = ls.empty();
  tracepoint(objectstore, collection_empty_exit, ret);
  return ret;
}
int CStore::collection_list(const coll_t& c, ghobject_t start, ghobject_t end,
			       bool sort_bitwise, int max,
			       vector<ghobject_t> *ls, ghobject_t *next)
{
  if (start.is_max())
    return 0;

  ghobject_t temp_next;
  if (!next)
    next = &temp_next;
  // figure out the pool id.  we need this in order to generate a
  // meaningful 'next' value.
  int64_t pool = -1;
  shard_id_t shard;
  {
    spg_t pgid;
    if (c.is_temp(&pgid)) {
      pool = -2 - pgid.pool();
      shard = pgid.shard;
    } else if (c.is_pg(&pgid)) {
      pool = pgid.pool();
      shard = pgid.shard;
    } else if (c.is_meta()) {
      pool = -1;
      shard = shard_id_t::NO_SHARD;
    } else {
      // hrm, the caller is test code!  we should get kill it off.  for now,
      // tolerate it.
      pool = 0;
      shard = shard_id_t::NO_SHARD;
    }
    dout(20) << __func__ << " pool is " << pool << " shard is " << shard
	     << " pgid " << pgid << dendl;
  }
  ghobject_t sep;
  sep.hobj.pool = -1;
  sep.set_shard(shard);
  if (!c.is_temp() && !c.is_meta()) {
    if (cmp_bitwise(start, sep) < 0) { // bitwise vs nibble doesn't matter here
      dout(10) << __func__ << " first checking temp pool" << dendl;
      coll_t temp = c.get_temp();
      int r = collection_list(temp, start, end, sort_bitwise, max, ls, next);
      if (r < 0)
	return r;
      if (*next != ghobject_t::get_max())
	return r;
      start = sep;
      dout(10) << __func__ << " fall through to non-temp collection, start "
	       << start << dendl;
    } else {
      dout(10) << __func__ << " start " << start << " >= sep " << sep << dendl;
    }
  }

  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0)
    return r;

  assert(NULL != index.index);
  RWLock::RLocker l((index.index)->access_lock);

  r = index->collection_list_partial(start, end, sort_bitwise, max, ls, next);

  if (r < 0) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  dout(20) << "objects: " << *ls << dendl;

  // CStoreHashIndex doesn't know the pool when constructing a 'next' value
  if (next && !next->is_max()) {
    next->hobj.pool = pool;
    next->set_shard(shard);
    dout(20) << "  next " << *next << dendl;
  }

  return 0;
}

int CStore::omap_get(const coll_t& _c, const ghobject_t &hoid,
			bufferlist *header,
			map<string, bufferlist> *out)
{
  tracepoint(objectstore, omap_get_enter, _c.c_str());
  const coll_t& c = !_need_temp_object_collection(_c, hoid) ? _c : _c.get_temp();
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0)
    return r;
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0)
      return r;
  }
  r = object_map->get(hoid, header, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  tracepoint(objectstore, omap_get_exit, 0);
  return 0;
}

int CStore::omap_get_header(
  const coll_t& _c,
  const ghobject_t &hoid,
  bufferlist *bl,
  bool allow_eio)
{
  tracepoint(objectstore, omap_get_header_enter, _c.c_str());
  const coll_t& c = !_need_temp_object_collection(_c, hoid) ? _c : _c.get_temp();
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0)
    return r;
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0)
      return r;
  }
  r = object_map->get_header(hoid, bl);
  if (r < 0 && r != -ENOENT) {
    assert(allow_eio || !m_filestore_fail_eio || r != -EIO);
    return r;
  }
  tracepoint(objectstore, omap_get_header_exit, 0);
  return 0;
}

int CStore::omap_get_keys(const coll_t& _c, const ghobject_t &hoid, set<string> *keys)
{
  tracepoint(objectstore, omap_get_keys_enter, _c.c_str());
  const coll_t& c = !_need_temp_object_collection(_c, hoid) ? _c : _c.get_temp();
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0)
    return r;
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0)
      return r;
  }
  r = object_map->get_keys(hoid, keys);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  tracepoint(objectstore, omap_get_keys_exit, 0);
  return 0;
}

int CStore::omap_get_values(const coll_t& _c, const ghobject_t &hoid,
			       const set<string> &keys,
			       map<string, bufferlist> *out)
{
  tracepoint(objectstore, omap_get_values_enter, _c.c_str());
  const coll_t& c = !_need_temp_object_collection(_c, hoid) ? _c : _c.get_temp();
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  CStoreIndex index;
  const char *where = 0;
  int r = get_index(c, &index);
  if (r < 0) {
    where = " (get_index)";
    goto out;
  }
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0) {
      where = " (lfn_find)";
      goto out;
    }
  }
  r = object_map->get_values(hoid, keys, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    goto out;
  }
  r = 0;
 out:
  tracepoint(objectstore, omap_get_values_exit, r);
  dout(15) << __func__ << " " << c << "/" << hoid << " = " << r
	   << where << dendl;
  return r;
}

int CStore::omap_check_keys(const coll_t& _c, const ghobject_t &hoid,
			       const set<string> &keys,
			       set<string> *out)
{
  tracepoint(objectstore, omap_check_keys_enter, _c.c_str());
  const coll_t& c = !_need_temp_object_collection(_c, hoid) ? _c : _c.get_temp();
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;

  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0)
    return r;
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0)
      return r;
  }
  r = object_map->check_keys(hoid, keys, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  tracepoint(objectstore, omap_check_keys_exit, 0);
  return 0;
}

ObjectMap::ObjectMapIterator CStore::get_omap_iterator(const coll_t& _c,
							  const ghobject_t &hoid)
{
  tracepoint(objectstore, get_omap_iterator, _c.c_str());
  const coll_t& c = !_need_temp_object_collection(_c, hoid) ? _c : _c.get_temp();
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  CStoreIndex index;
  int r = get_index(c, &index);
  if (r < 0) {
    dout(10) << __func__ << " " << c << "/" << hoid << " = 0 "
	     << "(get_index failed with " << cpp_strerror(r) << ")" << dendl;
    return CStoreObjectMap::CStoreObjectMapIterator();
  }
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0) {
      dout(10) << __func__ << " " << c << "/" << hoid << " = 0 "
	       << "(lfn_find failed with " << cpp_strerror(r) << ")" << dendl;
      return CStoreObjectMap::CStoreObjectMapIterator();
    }
  }
  return object_map->get_iterator(hoid);
}

int CStore::_collection_hint_expected_num_objs(const coll_t& c, uint32_t pg_num,
    uint64_t expected_num_objs,
    const CStoreSequencerPosition &spos)
{
  dout(15) << __func__ << " collection: " << c << " pg number: "
     << pg_num << " expected number of objects: " << expected_num_objs << dendl;

  if (!collection_empty(c) && !replaying) {
    dout(0) << "Failed to give an expected number of objects hint to collection : "
      << c << ", only empty collection can take such type of hint. " << dendl;
    return 0;
  }

  int ret;
  CStoreIndex index;
  ret = get_index(c, &index);
  if (ret < 0)
    return ret;
  // Pre-hash the collection
  ret = index->pre_hash_collection(pg_num, expected_num_objs);
  dout(10) << "pre_hash_collection " << c << " = " << ret << dendl;
  if (ret < 0)
    return ret;
  _set_replay_guard(c, spos);

  return 0;
}

int CStore::_create_collection(
  const coll_t& c,
  const CStoreSequencerPosition &spos)
{
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "create_collection " << fn << dendl;
  int r = ::mkdir(fn, 0755);
  if (r < 0)
    r = -errno;
  if (r == -EEXIST && replaying)
    r = 0;
  dout(10) << "create_collection " << fn << " = " << r << dendl;

  if (r < 0)
    return r;
  r = init_index(c);
  if (r < 0)
    return r;

  // create parallel temp collection, too
  if (!c.is_meta() && !c.is_temp()) {
    coll_t temp = c.get_temp();
    r = _create_collection(temp, spos);
    if (r < 0)
      return r;
  }

  _set_replay_guard(c, spos);
  return 0;
}

int CStore::_destroy_collection(const coll_t& c)
{
  int r = 0;
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "_destroy_collection " << fn << dendl;
  {
    CStoreIndex from;
    r = get_index(c, &from);
    if (r < 0)
      goto out;
    assert(NULL != from.index);
    RWLock::WLocker l((from.index)->access_lock);

    r = from->prep_delete();
    if (r < 0)
      goto out;
  }
  r = ::rmdir(fn);
  if (r < 0) {
    r = -errno;
    goto out;
  }

 out:
  // destroy parallel temp collection, too
  if (!c.is_meta() && !c.is_temp()) {
    coll_t temp = c.get_temp();
    int r2 = _destroy_collection(temp);
    if (r2 < 0) {
      r = r2;
      goto out_final;
    }
  }

 out_final:
  dout(10) << "_destroy_collection " << fn << " = " << r << dendl;
  return r;
}


int CStore::_collection_add(const coll_t& c, const coll_t& oldcid, const ghobject_t& o,
			       const CStoreSequencerPosition& spos)
{
  dout(15) << "collection_add " << c << "/" << o << " from " << oldcid << "/" << o << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(o)) {
    dout(20) << __func__ << " object " << o << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(o);
  op_lock.Unlock();

	map_header_t *mh = NULL;
	bufferlist bl;
	int r, srccmp, dstcmp;
  CFDRef fd;
  dstcmp = _check_replay_guard(c, o, spos);
  if (dstcmp < 0) {
		r = 0;
		goto out;
	}

  // check the src name too; it might have a newer guard, and we don't
  // want to clobber it
  srccmp = _check_replay_guard(oldcid, o, spos);
  if (srccmp < 0) {
		r = 0;
		goto out;
	}

  // open guard on object so we don't any previous operations on the
  // new name that will modify the source inode.
  r = lfn_open(oldcid, o, 0, &fd);
  if (r < 0) {
    // the source collection/object does not exist. If we are replaying, we
    // should be safe, so just return 0 and move on.
    assert(replaying);
    dout(10) << "collection_add " << c << "/" << o << " from "
	     << oldcid << "/" << o << " (dne, continue replay) " << dendl;
    goto out;
  }
  if (dstcmp > 0) {      // if dstcmp == 0 the guard already says "in-progress"
    _set_replay_guard(**fd, spos, &o, true);
  }

  r = lfn_link(oldcid, c, o, o);
  if (replaying && !backend->can_checkpoint() &&
      r == -EEXIST)    // crashed between link() and set_replay_guard()
    r = 0;

  _inject_failure();

  // close guard on object so we don't do this again
  if (r == 0) {
    _close_replay_guard(**fd, spos);
  }
  lfn_close(fd);


	mh = new map_header_t(c, o);
	::encode(*mh, bl);
	r = object_map->set_map(o, bl);
	if (r < 0)
		goto out;

out:
  dout(10) << "collection_add " << c << "/" << o << " from " << oldcid << "/" << o << " = " << r << dendl;
  
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(o);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}

int CStore::_collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
				       coll_t c, const ghobject_t& o,
				       const CStoreSequencerPosition& spos,
				       bool allow_enoent)
{
  dout(15) << __func__ << " " << c << "/" << o << " from " << oldcid << "/" << oldoid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oldoid)) {
    dout(20) << __func__ << " object " << oldoid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oldoid);
  op_lock.Unlock();

  int r = 0;
  int dstcmp, srccmp;
	bufferlist bl;
	map_header_t *mh = NULL;

  if (replaying) {
    /* If the destination collection doesn't exist during replay,
     * we need to delete the src object and continue on
     */
    if (!collection_exists(c))
      goto out_rm_src;
  }

  dstcmp = _check_replay_guard(c, o, spos);
  if (dstcmp < 0)
    goto out_rm_src;

  // check the src name too; it might have a newer guard, and we don't
  // want to clobber it
  srccmp = _check_replay_guard(oldcid, oldoid, spos);
  if (srccmp < 0) 
    goto out_rm_src;

  {
    // open guard on object so we don't any previous operations on the
    // new name that will modify the source inode.
    CFDRef fd;
    r = lfn_open(oldcid, oldoid, 0, &fd);
    if (r < 0) {
      // the source collection/object does not exist. If we are replaying, we
      // should be safe, so just return 0 and move on.
      if (replaying) {
	dout(10) << __func__ << " " << c << "/" << o << " from "
		 << oldcid << "/" << oldoid << " (dne, continue replay) " << dendl;
      } else if (allow_enoent) {
	dout(10) << __func__ << " " << c << "/" << o << " from "
		 << oldcid << "/" << oldoid << " (dne, ignoring enoent)"
		 << dendl;
      } else {
	assert(0 == "ERROR: source must exist");
      }
      goto out_rm_src;
    }
    if (dstcmp > 0) {      // if dstcmp == 0 the guard already says "in-progress"
      _set_replay_guard(**fd, spos, &o, true);
    }

    r = lfn_link(oldcid, c, oldoid, o);
    if (replaying && !backend->can_checkpoint() &&
	r == -EEXIST)    // crashed between link() and set_replay_guard()
      r = 0;

    _inject_failure();

    if (r == 0) {
      // the name changed; link the omap content
      r = object_map->clone(oldoid, o, &spos);
      if (r == -ENOENT)
	r = 0;
    }

		mh = new map_header_t(c, o);
		::encode(*mh, bl);
		if (r == 0) {
			r = object_map->set_map(o, bl);
		}

    _inject_failure();

    lfn_close(fd);
    fd = CFDRef();

    if (r == 0)
      r = lfn_unlink(oldcid, oldoid, spos, true);

    if (r == 0)
      r = lfn_open(c, o, 0, &fd);

		if (r == 0)
			r = object_map->remove_map(oldoid);

    // close guard on object so we don't do this again
    if (r == 0) {
      _close_replay_guard(**fd, spos);
      lfn_close(fd);
    }
  }

  dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/" << oldoid
	   << " = " << r << dendl;
  
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oldoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;

 out_rm_src:
  // remove source
  if (_check_replay_guard(oldcid, oldoid, spos) > 0) {
    r = lfn_unlink(oldcid, oldoid, spos, true);
  }

  dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/" << oldoid
	   << " = " << r << dendl;
  
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oldoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}

void CStore::_inject_failure()
{
  if (m_filestore_kill_at.read()) {
    int final = m_filestore_kill_at.dec();
    dout(5) << "_inject_failure " << (final+1) << " -> " << final << dendl;
    if (final == 0) {
      derr << "_inject_failure KILLING" << dendl;
      g_ceph_context->_log->flush();
      _exit(1);
    }
  }
}

int CStore::_omap_clear(const coll_t& cid, const ghobject_t &hoid,
			   const CStoreSequencerPosition &spos) {
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(hoid)) {
    dout(20) << __func__ << " object " << hoid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(hoid);
  op_lock.Unlock();

  CStoreIndex index;
  int r = get_index(cid, &index);
  if (r < 0)
    goto out;
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0)
      goto out;
  }
  r = object_map->clear_keys_header(hoid, &spos);
  if (r < 0 && r != -ENOENT)
    goto out;
  
out:
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(hoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}

int CStore::_omap_setkeys(const coll_t& cid, const ghobject_t &hoid,
			     const map<string, bufferlist> &aset,
			     const CStoreSequencerPosition &spos) {
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(hoid)) {
    dout(20) << __func__ << " object " << hoid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(hoid);
  op_lock.Unlock();

  CStoreIndex index;
  int r;

	if (hoid.is_pgmeta())
		goto skip;

  r = get_index(cid, &index);
  if (r < 0) {
		dout(20) << __func__ << " get_index got " << cpp_strerror(r) << dendl;
    goto out;
	}
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0) {
			dout(20) << __func__ << " lfn_find got " << cpp_strerror(r) << dendl;
      goto out;
		}
  }
skip:
  r = object_map->set_keys(hoid, aset, &spos);
out:	
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(hoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }
  
	dout(20) << __func__ << " " << cid << "/" << hoid << " = " << r << dendl;
	return r;
}

int CStore::_omap_rmkeys(const coll_t& cid, const ghobject_t &hoid,
					 const set<string> &keys,
					 const CStoreSequencerPosition &spos) {
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(hoid)) {
    dout(20) << __func__ << " object " << hoid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(hoid);
  op_lock.Unlock();

  CStoreIndex index;
  int r;
  //treat pgmeta as a logical object, skip to check exist
  if (hoid.is_pgmeta())
    goto skip;

  r = get_index(cid, &index);
  if (r < 0)
    goto out;
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0)
      goto out;
  }
skip:
  r = object_map->rm_keys(hoid, keys, &spos);
out:	
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(hoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  if (r < 0 && r != -ENOENT)
    return r;
  return 0;
}

int CStore::_omap_rmkeyrange(const coll_t& cid, const ghobject_t &hoid,
				const string& first, const string& last,
				const CStoreSequencerPosition &spos) {
  dout(15) << __func__ << " " << cid << "/" << hoid << " [" << first << "," << last << "]" << dendl;
  set<string> keys;
  {
    ObjectMap::ObjectMapIterator iter = get_omap_iterator(cid, hoid);
    if (!iter)
      return -ENOENT;
    for (iter->lower_bound(first); iter->valid() && iter->key() < last;
	 iter->next()) {
      keys.insert(iter->key());
    }
  }
  return _omap_rmkeys(cid, hoid, keys, spos);
}

int CStore::_omap_setheader(const coll_t& cid, const ghobject_t &hoid,
			       const bufferlist &bl,
			       const CStoreSequencerPosition &spos)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(hoid)) {
    dout(20) << __func__ << " object " << hoid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(hoid);
  op_lock.Unlock();

  CStoreIndex index;
  int r = get_index(cid, &index);
  if (r < 0)
    goto out;
  {
    assert(NULL != index.index);
    RWLock::RLocker l((index.index)->access_lock);
    r = lfn_find(hoid, index);
    if (r < 0)
      goto out;
  }
  
  r = object_map->set_header(hoid, bl, &spos);

out:
  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(hoid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return r;
}

int CStore::_split_collection(const coll_t& cid,
				 uint32_t bits,
				 uint32_t rem,
				 coll_t dest,
				 const CStoreSequencerPosition &spos)
{
	// stop compress while pg splits
	comp_tp.pause();

	vector<ghobject_t> updated;
	ghobject_t n;
  int r = 0;
	bufferlist bl;
	bufferlist::iterator p;
	map_header_t *mh = NULL;

  {
    dout(15) << __func__ << " " << cid << " bits: " << bits << dendl;
    if (!collection_exists(cid)) {
      dout(2) << __func__ << ": " << cid << " DNE" << dendl;
      assert(replaying);
      goto out;
    }
    if (!collection_exists(dest)) {
      dout(2) << __func__ << ": " << dest << " DNE" << dendl;
      assert(replaying);
      goto out;
    }

    int dstcmp = _check_replay_guard(dest, spos);
    if (dstcmp < 0)
      goto out;

    int srccmp = _check_replay_guard(cid, spos);
    if (srccmp < 0)
      goto out;

    _set_global_replay_guard(cid, spos);
    _set_replay_guard(cid, spos, true);
    _set_replay_guard(dest, spos, true);

    CStoreIndex from;
    r = get_index(cid, &from);

    CStoreIndex to;
    if (!r)
      r = get_index(dest, &to);

    if (!r) {
      assert(NULL != from.index);
      RWLock::WLocker l1((from.index)->access_lock);

      assert(NULL != to.index);
      RWLock::WLocker l2((to.index)->access_lock);

      r = from->split(rem, bits, to.index);
    }

    _close_replay_guard(cid, spos);
    _close_replay_guard(dest, spos);
  }
	// update object header
	while(1) {
		collection_list(dest, n, ghobject_t::get_max(), true, get_ideal_list_max(), &updated, &n);
		dout(20) << __func__ << " updated size " << updated.size() << dendl;
		if (updated.empty())
			break;
		for(vector<ghobject_t>::iterator it = updated.begin(); it != updated.end(); ++it) {
			dout(20) << __func__ << ": " << *it << " now in dest " << dest << dendl;
			assert(it->match(bits, rem));
			mh = new map_header_t(dest, *it);
			::encode(*mh, bl);
			r = object_map->set_map(*it, bl);
			if (r < 0) {
				derr << __func__ << " update header error " << dendl;
				goto out;
			}
			bl.clear();
		}
		updated.clear();
	}
  if (g_conf->filestore_debug_verify_split) {
    vector<ghobject_t> objects;
    ghobject_t next;
    while (1) {
      collection_list(
	cid,
	next, ghobject_t::get_max(),
	true,
	get_ideal_list_max(),
	&objects,
	&next);
      if (objects.empty())
	break;
      for (vector<ghobject_t>::iterator i = objects.begin();
	   i != objects.end();
	   ++i) {
	dout(20) << __func__ << ": " << *i << " still in source "
		 << cid << dendl;
	assert(!i->match(bits, rem));
      }
      objects.clear();
    }
    next = ghobject_t();
    while (1) {
      collection_list(
	dest,
	next, ghobject_t::get_max(),
	true,
	get_ideal_list_max(),
	&objects,
	&next);
      if (objects.empty())
	break;
      for (vector<ghobject_t>::iterator i = objects.begin();
	   i != objects.end();
	   ++i) {
	dout(20) << __func__ << ": " << *i << " now in dest "
		 << *i << dendl;
	assert(i->match(bits, rem));
      }
      objects.clear();
    }
  }

out:
	comp_tp.unpause();
  return r;
}

int CStore::_set_alloc_hint(const coll_t& cid, const ghobject_t& oid,
                               uint64_t expected_object_size,
                               uint64_t expected_write_size)
{
  dout(15) << "set_alloc_hint " << cid << "/" << oid << " object_size " << expected_object_size << " write_size " << expected_write_size << dendl;

  op_lock.Lock();
  while(in_progress_comp.count(oid)) {
    dout(20) << __func__ << " object " << oid << " is in progress compress, wait ..." << dendl;
    op_cond.Wait(op_lock);
  }
  in_progress_op.insert(oid);
  op_lock.Unlock();

  CFDRef fd;
  int ret;

  ret = lfn_open(cid, oid, false, &fd);
  if (ret < 0)
    goto out;

  {
    // TODO: a more elaborate hint calculation
    uint64_t hint = MIN(expected_write_size, m_filestore_max_alloc_hint_size);

    ret = backend->set_alloc_hint(**fd, hint);
    dout(20) << "set_alloc_hint hint " << hint << " ret " << ret << dendl;
  }

  lfn_close(fd);
out:
  dout(10) << "set_alloc_hint " << cid << "/" << oid << " object_size " << expected_object_size << " write_size " << expected_write_size << " = " << ret << dendl;
  assert(!m_filestore_fail_eio || ret != -EIO);

  {
    Mutex::Locker l(op_lock);
    in_progress_op.erase(oid);
  }
  {
    Mutex::Locker l(comp_lock);
    comp_cond.SignalAll();
  }

  return ret;
}

const char** CStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "filestore_min_sync_interval",
    "filestore_max_sync_interval",
    "filestore_queue_max_ops",
    "filestore_queue_max_bytes",
    "filestore_queue_max_ops",
    "filestore_expected_throughput_bytes",
    "filestore_expected_throughput_ops",
    "filestore_queue_low_threshhold",
    "filestore_queue_high_threshhold",
    "filestore_queue_high_delay_multiple",
    "filestore_queue_max_delay_multiple",
    "filestore_commit_timeout",
    "filestore_dump_file",
    "filestore_kill_at",
    "filestore_fail_eio",
    "filestore_fadvise",
    "filestore_sloppy_crc",
    "filestore_sloppy_crc_block_size",
    "filestore_max_alloc_hint_size",
    NULL
  };
  return KEYS;
}

void CStore::handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed)
{
  if (changed.count("filestore_max_inline_xattr_size") ||
      changed.count("filestore_max_inline_xattr_size_xfs") ||
      changed.count("filestore_max_inline_xattr_size_btrfs") ||
      changed.count("filestore_max_inline_xattr_size_other") ||
      changed.count("filestore_max_inline_xattrs") ||
      changed.count("filestore_max_inline_xattrs_xfs") ||
      changed.count("filestore_max_inline_xattrs_btrfs") ||
      changed.count("filestore_max_inline_xattrs_other")) {
    Mutex::Locker l(lock);
    set_xattr_limits_via_conf();
  }

  if (changed.count("filestore_queue_max_bytes") ||
      changed.count("filestore_queue_max_ops") ||
      changed.count("filestore_expected_throughput_bytes") ||
      changed.count("filestore_expected_throughput_ops") ||
      changed.count("filestore_queue_low_threshhold") ||
      changed.count("filestore_queue_high_threshhold") ||
      changed.count("filestore_queue_high_delay_multiple") ||
      changed.count("filestore_queue_max_delay_multiple")) {
    Mutex::Locker l(lock);
    set_throttle_params();
  }

  if (changed.count("filestore_min_sync_interval") ||
      changed.count("filestore_max_sync_interval") ||
      changed.count("filestore_kill_at") ||
      changed.count("filestore_fail_eio") ||
      changed.count("filestore_sloppy_crc") ||
      changed.count("filestore_sloppy_crc_block_size") ||
      changed.count("filestore_max_alloc_hint_size") ||
      changed.count("filestore_fadvise")) {
    Mutex::Locker l(lock);
    m_filestore_min_sync_interval = conf->filestore_min_sync_interval;
    m_filestore_max_sync_interval = conf->filestore_max_sync_interval;
    m_filestore_kill_at.set(conf->filestore_kill_at);
    m_filestore_fail_eio = conf->filestore_fail_eio;
    m_filestore_fadvise = conf->filestore_fadvise;
    m_filestore_sloppy_crc = conf->filestore_sloppy_crc;
    m_filestore_sloppy_crc_block_size = conf->filestore_sloppy_crc_block_size;
    m_filestore_max_alloc_hint_size = conf->filestore_max_alloc_hint_size;
  }
  if (changed.count("filestore_commit_timeout")) {
    Mutex::Locker l(timer_lock);
    m_filestore_commit_timeout = conf->filestore_commit_timeout;
  }
  if (changed.count("filestore_dump_file")) {
    if (conf->filestore_dump_file.length() &&
	conf->filestore_dump_file != "-") {
      dump_start(conf->filestore_dump_file);
    } else {
      dump_stop();
    }
  }
}

int CStore::set_throttle_params()
{
  stringstream ss;
  bool valid = throttle_bytes.set_params(
    g_conf->filestore_queue_low_threshhold,
    g_conf->filestore_queue_high_threshhold,
    g_conf->filestore_expected_throughput_bytes,
    g_conf->filestore_queue_high_delay_multiple,
    g_conf->filestore_queue_max_delay_multiple,
    g_conf->filestore_queue_max_bytes,
    &ss);

  valid &= throttle_ops.set_params(
    g_conf->filestore_queue_low_threshhold,
    g_conf->filestore_queue_high_threshhold,
    g_conf->filestore_expected_throughput_ops,
    g_conf->filestore_queue_high_delay_multiple,
    g_conf->filestore_queue_max_delay_multiple,
    g_conf->filestore_queue_max_ops,
    &ss);

  logger->set(l_os_oq_max_ops, throttle_ops.get_max());
  logger->set(l_os_oq_max_bytes, throttle_bytes.get_max());

  if (!valid) {
    derr << "tried to set invalid params: "
	 << ss.str()
	 << dendl;
  }
  return valid ? 0 : -EINVAL;
}

void CStore::dump_start(const std::string& file)
{
  dout(10) << "dump_start " << file << dendl;
  if (m_filestore_do_dump) {
    dump_stop();
  }
  m_filestore_dump_fmt.reset();
  m_filestore_dump_fmt.open_array_section("dump");
  m_filestore_dump.open(file.c_str());
  m_filestore_do_dump = true;
}

void CStore::dump_stop()
{
  dout(10) << "dump_stop" << dendl;
  m_filestore_do_dump = false;
  if (m_filestore_dump.is_open()) {
    m_filestore_dump_fmt.close_section();
    m_filestore_dump_fmt.flush(m_filestore_dump);
    m_filestore_dump.flush();
    m_filestore_dump.close();
  }
}

void CStore::dump_transactions(vector<ObjectStore::Transaction>& ls, uint64_t seq, OpSequencer *osr)
{
  m_filestore_dump_fmt.open_array_section("transactions");
  unsigned trans_num = 0;
  for (vector<ObjectStore::Transaction>::iterator i = ls.begin(); i != ls.end(); ++i, ++trans_num) {
    m_filestore_dump_fmt.open_object_section("transaction");
    m_filestore_dump_fmt.dump_string("osr", osr->get_name());
    m_filestore_dump_fmt.dump_unsigned("seq", seq);
    m_filestore_dump_fmt.dump_unsigned("trans_num", trans_num);
    (*i).dump(&m_filestore_dump_fmt);
    m_filestore_dump_fmt.close_section();
  }
  m_filestore_dump_fmt.close_section();
  m_filestore_dump_fmt.flush(m_filestore_dump);
  m_filestore_dump.flush();
}

void CStore::set_xattr_limits_via_conf()
{
  uint32_t fs_xattr_size;
  uint32_t fs_xattrs;
  uint32_t fs_xattr_max_value_size;

  switch (m_fs_type) {
#if defined(__linux__)
  case XFS_SUPER_MAGIC:
    fs_xattr_size = g_conf->filestore_max_inline_xattr_size_xfs;
    fs_xattrs = g_conf->filestore_max_inline_xattrs_xfs;
    fs_xattr_max_value_size = g_conf->filestore_max_xattr_value_size_xfs;
    break;
  case BTRFS_SUPER_MAGIC:
    fs_xattr_size = g_conf->filestore_max_inline_xattr_size_btrfs;
    fs_xattrs = g_conf->filestore_max_inline_xattrs_btrfs;
    fs_xattr_max_value_size = g_conf->filestore_max_xattr_value_size_btrfs;
    break;
#endif
  default:
    fs_xattr_size = g_conf->filestore_max_inline_xattr_size_other;
    fs_xattrs = g_conf->filestore_max_inline_xattrs_other;
    fs_xattr_max_value_size = g_conf->filestore_max_xattr_value_size_other;
    break;
  }

  // Use override value if set
  if (g_conf->filestore_max_inline_xattr_size)
    m_filestore_max_inline_xattr_size = g_conf->filestore_max_inline_xattr_size;
  else
    m_filestore_max_inline_xattr_size = fs_xattr_size;

  // Use override value if set
  if (g_conf->filestore_max_inline_xattrs)
    m_filestore_max_inline_xattrs = g_conf->filestore_max_inline_xattrs;
  else
    m_filestore_max_inline_xattrs = fs_xattrs;

  // Use override value if set
  if (g_conf->filestore_max_xattr_value_size)
    m_filestore_max_xattr_value_size = g_conf->filestore_max_xattr_value_size;
  else
    m_filestore_max_xattr_value_size = fs_xattr_max_value_size;

  if (m_filestore_max_xattr_value_size < g_conf->osd_max_object_name_len) {
    derr << "WARNING: max attr value size ("
	 << m_filestore_max_xattr_value_size
	 << ") is smaller than osd_max_object_name_len ("
	 << g_conf->osd_max_object_name_len
	 << ").  Your backend filesystem appears to not support attrs large "
	 << "enough to handle the configured max rados name size.  You may get "
	 << "unexpected ENAMETOOLONG errors on rados operations or buggy "
	 << "behavior"
	 << dendl;
  }
}

// -- CFSSuperblock --

void CFSSuperblock::encode(bufferlist &bl) const
{
  ENCODE_START(2, 1, bl);
  compat_features.encode(bl);
  ::encode(omap_backend, bl);
  ENCODE_FINISH(bl);
}

void CFSSuperblock::decode(bufferlist::iterator &bl)
{
  DECODE_START(2, bl);
  compat_features.decode(bl);
  if (struct_v >= 2)
    ::decode(omap_backend, bl);
  else
    omap_backend = "leveldb";
  DECODE_FINISH(bl);
}

void CFSSuperblock::dump(Formatter *f) const
{
  f->open_object_section("compat");
  compat_features.dump(f);
  f->dump_string("omap_backend", omap_backend);
  f->close_section();
}

void CFSSuperblock::generate_test_instances(list<CFSSuperblock*>& o)
{
  CFSSuperblock z;
  o.push_back(new CFSSuperblock(z));
  CompatSet::FeatureSet feature_compat;
  CompatSet::FeatureSet feature_ro_compat;
  CompatSet::FeatureSet feature_incompat;
  feature_incompat.insert(CEPH_FS_FEATURE_INCOMPAT_SHARDS);
  z.compat_features = CompatSet(feature_compat, feature_ro_compat,
                                feature_incompat);
  o.push_back(new CFSSuperblock(z));
  z.omap_backend = "rocksdb";
  o.push_back(new CFSSuperblock(z));
}
