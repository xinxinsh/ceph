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

#include <iostream>
#include <map>

#include "include/compat.h"
#include "include/linux_fiemap.h"

#include "common/xattr.h"
#include "chain_xattr.h"

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN


#include <fstream>
#include <sstream>

#include "KeyValueStore.h"
#include "common/BackTrace.h"
#include "include/types.h"

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
#include "DBObjectMap.h"
#include "LevelDBStore.h"

#include "common/ceph_crypto.h"
using ceph::crypto::SHA1;

#include "include/assert.h"

#include "common/config.h"

#define dout_subsys ceph_subsys_keyvaluestore
// In order to make right ordering for leveldb matching with hobject_t
#define SEP_C '!'
#define SEP_S "!"


uint64_t KeyValueStore::SubmitManager::op_submit_start()
{
  lock.Lock();
  uint64_t op = ++op_seq;
  dout(10) << "op_submit_start " << op << dendl;
  return op;
}

void KeyValueStore::SubmitManager::op_submit_finish(uint64_t op)
{
  dout(10) << "op_submit_finish " << op << dendl;
  if (op != op_submitted + 1) {
      dout(0) << "op_submit_finish " << op << " expected " << (op_submitted + 1)
          << ", OUT OF ORDER" << dendl;
      assert(0 == "out of order op_submit_finish");
  }
  op_submitted = op;
  lock.Unlock();
}

int KeyValueStore::get_cdir(coll_t cid, char *s, int len)
{
  const string &cid_str(cid.to_str());
  return snprintf(s, len, "%s/current/%s", basedir.c_str(), cid_str.c_str());
}

static void append_escaped(const string &in, string *out)
{
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    if (*i == '%') {
      out->push_back('%');
      out->push_back('p');
    } else if (*i == '.') {
      out->push_back('%');
      out->push_back('e');
    } else if (*i == SEP_C) {
      out->push_back('%');
      out->push_back('u');
    } else if (*i == '!') {
      out->push_back('%');
      out->push_back('s');
    } else {
      out->push_back(*i);
    }
  }
}

static bool append_unescaped(string::const_iterator begin,
                             string::const_iterator end,
                             string *out) {
  for (string::const_iterator i = begin; i != end; ++i) {
    if (*i == '%') {
      ++i;
      if (*i == 'p')
        out->push_back('%');
      else if (*i == 'e')
        out->push_back('.');
      else if (*i == 'u')
        out->push_back(SEP_C);
      else if (*i == 's')
        out->push_back('!');
      else
        return false;
    } else {
      out->push_back(*i);
    }
  }
  return true;
}

string KeyValueStore::calculate_key(coll_t cid)
{
  string full_name;

  append_escaped(cid.to_str(), &full_name);
  full_name.append(SEP_S);
  return full_name;
}

string KeyValueStore::calculate_key(coll_t cid, const ghobject_t& oid)
{
  string full_name;

  append_escaped(cid.to_str(), &full_name);
  full_name.append(SEP_S);

  char buf[PATH_MAX];
  char *t = buf;
  char *end = t + sizeof(buf);

  // make field ordering match with hobject_t compare operations
  snprintf(t, end - t, "%.*X", (int)(sizeof(oid.hobj.hash)*2),
           (uint32_t)oid.get_filestore_key_u32());
  full_name += string(buf);
  full_name.append(SEP_S);

  append_escaped(oid.hobj.nspace, &full_name);
  full_name.append(SEP_S);

  t = buf;
  if (oid.hobj.pool == -1)
    t += snprintf(t, end - t, "none");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)oid.hobj.pool);
  full_name += string(buf);
  full_name.append(SEP_S);

  append_escaped(oid.hobj.get_key(), &full_name);
  full_name.append(SEP_S);

  append_escaped(oid.hobj.oid.name, &full_name);
  full_name.append(SEP_S);

  t = buf;
  if (oid.hobj.snap == CEPH_NOSNAP)
    t += snprintf(t, end - t, "head");
  else if (oid.hobj.snap == CEPH_SNAPDIR)
    t += snprintf(t, end - t, "snapdir");
  else
    t += snprintf(t, end - t, "%llx", (long long unsigned)oid.hobj.snap);
  full_name += string(buf);

  if (oid.generation != ghobject_t::NO_GEN) {
    assert(oid.shard_id != ghobject_t::NO_SHARD);
    full_name.append(SEP_S);

    t = buf;
    end = t + sizeof(buf);
    t += snprintf(t, end - t, "%llx", (long long unsigned)oid.generation);
    full_name += string(buf);

    full_name.append(SEP_S);

    t = buf;
    end = t + sizeof(buf);
    t += snprintf(t, end - t, "%x", (int)oid.shard_id);
    full_name += string(buf);
  }

  return full_name;
}

bool KeyValueStore::parse_object(const string &long_name, coll_t *out_coll,
                                 ghobject_t *out)
{
  string coll;
  string name;
  string key;
  string ns;
  uint32_t hash;
  snapid_t snap;
  uint64_t pool;
  gen_t generation = ghobject_t::NO_GEN;
  shard_t shard_id = ghobject_t::NO_SHARD;

  string::const_iterator current = long_name.begin();
  string::const_iterator end;

  for (end = current; end != long_name.end() && *end != SEP_C; ++end) ;
  if (!append_unescaped(current, end, &coll))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
  if (end == long_name.end())
    return false;
  string hash_str(current, end);
  sscanf(hash_str.c_str(), "%X", &hash);

  current = ++end;
  for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &ns))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
  if (end == long_name.end())
    return false;
  string pstring(current, end);
  if (pstring == "none")
    pool = (uint64_t)-1;
  else
    pool = strtoull(pstring.c_str(), NULL, 16);

  current = ++end;
  for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &key))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
  if (end == long_name.end())
    return false;
  if (!append_unescaped(current, end, &name))
    return false;

  current = ++end;
  for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
  string snap_str(current, end);
  if (snap_str == "head")
    snap = CEPH_NOSNAP;
  else if (snap_str == "snapdir")
    snap = CEPH_SNAPDIR;
  else
    snap = strtoull(snap_str.c_str(), NULL, 16);

  // Optional generation/shard_id
  string genstring, shardstring;
  if (end != long_name.end()) {
    current = ++end;
    for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
    if (end == long_name.end())
      return false;
    genstring = string(current, end);

    generation = (gen_t)strtoull(genstring.c_str(), NULL, 16);

    current = ++end;
    for ( ; end != long_name.end() && *end != SEP_C; ++end) ;
    if (end != long_name.end())
      return false;
    shardstring = string(current, end);

    shard_id = (shard_t)strtoul(shardstring.c_str(), NULL, 16);
  }

  if (out) {
    (*out) = ghobject_t(hobject_t(name, key, snap, hash, (int64_t)pool, ns),
                        generation, shard_id);
    // restore reversed hash. see calculate_key
    out->hobj.hash = out->get_filestore_key();
  }

  if (out_coll)
    *out_coll = coll_t(coll);

  return true;
}

const string KeyValueStore::OBJECT = "__OBJ__";
const string KeyValueStore::COLLECTION = "__COLLECTION__";
const string KeyValueStore::COLLECTION_ATTR = "__COLL_ATTR__";

string KeyValueStore::collection_attr_prefix(coll_t c)
{
  char buf[100];
  snprintf(buf, sizeof(buf), "%s%s__", COLLECTION_ATTR.c_str(),
           c.to_str().c_str());
  return string(buf);
}


ostream& operator<<(ostream& out, const KeyValueStore::OpSequencer& s)
{
  assert(&out);
  return out << *s.parent;
}

int KeyValueStore::_create_current()
{
  struct stat st;
  int ret = ::stat(current_fn.c_str(), &st);
  if (ret == 0) {
    // current/ exists
    if (!S_ISDIR(st.st_mode)) {
      dout(0) << "_create_current: current/ exists but is not a directory" << dendl;
      ret = -EINVAL;
    }
  } else {
    ret = ::mkdir(current_fn.c_str(), 0755);
    if (ret < 0) {
      ret = -errno;
      dout(0) << "_create_current: mkdir " << current_fn << " failed: "<< cpp_strerror(ret) << dendl;
    }
  }

  return ret;
}

KeyValueStore::KeyValueStore(const std::string &base, const std::string &jdev, const char *name, bool do_update) :
  internal_name(name),
  basedir(base),
  fsid_fd(-1), op_fd(-1),
  basedir_fd(-1), current_fd(-1),
  kv_type(KV_TYPE_NONE),
  backend(NULL),
  ondisk_finisher(g_ceph_context),
  lock("KeyValueStore::lock"),
  default_osr("default"),
  op_queue_len(0), op_queue_bytes(0),
  op_finisher(g_ceph_context),
  op_tp(g_ceph_context, "KeyValueStore::op_tp",
        g_conf->filestore_op_threads, "keyvaluestore_op_threads"),
  op_wq(this, g_conf->filestore_op_thread_timeout,
        g_conf->filestore_op_thread_suicide_timeout, &op_tp),
  logger(NULL),
  read_error_lock("KeyValueStore::read_error_lock"),
  m_filestore_fail_eio(g_conf->filestore_fail_eio),
  do_update(do_update)
{
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
  PerfCountersBuilder plb(g_ceph_context, internal_name, 0, 1);
  logger = plb.create_perf_counters();

  g_ceph_context->get_perfcounters_collection()->add(logger);
  g_ceph_context->_conf->add_observer(this);
}

KeyValueStore::~KeyValueStore()
{
  g_ceph_context->_conf->remove_observer(this);
  g_ceph_context->get_perfcounters_collection()->remove(logger);

  delete logger;
}

int KeyValueStore::statfs(struct statfs *buf)
{
  if (::statfs(basedir.c_str(), buf) < 0) {
    int r = -errno;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int KeyValueStore::mkfs()
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
      ret = errno;
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

  ret = _create_current();
  if (ret < 0) {
    derr << "mkfs: failed to create current/ " << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }

  // write initial op_seq
  {
    uint64_t initial_seq = 0;
    int fd = read_op_seq(&initial_seq);
    if (fd < 0) {
      derr << "mkfs: failed to create " << current_op_seq_fn << ": "
	   << cpp_strerror(fd) << dendl;
      goto close_fsid_fd;
    }
    if (initial_seq == 0) {
      int err = write_op_seq(fd, 1);
      if (err < 0) {
	TEMP_FAILURE_RETRY(::close(fd));
	derr << "mkfs: failed to write to " << current_op_seq_fn << ": "
	     << cpp_strerror(err) << dendl;
	goto close_fsid_fd;
      }
    }
    TEMP_FAILURE_RETRY(::close(fd));
  }

  if (_detect_backend()) {
    derr << "KeyValueStore::mkfs error in _detect_backend" << dendl;
    ret = -1;
    goto close_fsid_fd;
  }

  {
    KeyValueDB *store;
    if (kv_type == KV_TYPE_LEVELDB) {
      store = new LevelDBStore(g_ceph_context, current_fn);
    } else {
      derr << "KeyValueStore::mount error: unknown backend type" << kv_type << dendl;
      ret = -1;
      goto close_fsid_fd;
    }

    stringstream err;
    if (store->init(err, true)) {
      delete store;
      derr << "mkfs failed to create keyvaluestore backend: " << err.str() << dendl;
      ret = -1;
      goto close_fsid_fd;
    } else {
      delete store;
      dout(1) << "keyvaluestore backend exists/created" << dendl;
    }
  }

  {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::DB *db;
    leveldb::Status status = leveldb::DB::Open(options, omap_dir, &db);
    if (status.ok()) {
      delete db;
      dout(1) << "leveldb db exists/created" << dendl;
    } else {
      derr << "mkfs failed to create omap leveldb: " << status.ToString() << dendl;
      ret = -1;
      goto close_fsid_fd;
    }
  }

  dout(1) << "mkfs done in " << basedir << dendl;
  ret = 0;

 close_fsid_fd:
  TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
 close_basedir_fd:
  TEMP_FAILURE_RETRY(::close(basedir_fd));
  return ret;
}

int KeyValueStore::read_fsid(int fd, uuid_d *uuid)
{
  char fsid_str[40];
  int ret = safe_read(fd, fsid_str, sizeof(fsid_str));
  if (ret < 0)
    return ret;
  if (ret == 8) {
    // old 64-bit fsid... mirror it.
    *(uint64_t*)&uuid->uuid[0] = *(uint64_t*)fsid_str;
    *(uint64_t*)&uuid->uuid[8] = *(uint64_t*)fsid_str;
    return 0;
  }

  if (ret > 36)
    fsid_str[36] = 0;
  if (!uuid->parse(fsid_str))
    return -EINVAL;
  return 0;
}

int KeyValueStore::lock_fsid()
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

bool KeyValueStore::test_mount_in_use()
{
  dout(5) << "test_mount basedir " << basedir << dendl;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());

  // verify fs isn't in use

  fsid_fd = ::open(fn, O_RDWR, 0644);
  if (fsid_fd < 0)
    return 0;   // no fsid, ok.
  bool inuse = lock_fsid() < 0;
  TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
  return inuse;
}

int KeyValueStore::update_version_stamp()
{
  return write_version_stamp();
}

int KeyValueStore::version_stamp_is_valid(uint32_t *version)
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
  bl.push_back(bp);
  bufferlist::iterator i = bl.begin();
  ::decode(*version, i);
  if (*version == target_version)
    return 1;
  else
    return 0;
}

int KeyValueStore::write_version_stamp()
{
  bufferlist bl;
  ::encode(target_version, bl);

  return safe_write_file(basedir.c_str(), "store_version",
      bl.c_str(), bl.length());
}

int KeyValueStore::read_op_seq(uint64_t *seq)
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
    TEMP_FAILURE_RETRY(::close(op_fd));
    assert(!m_filestore_fail_eio || ret != -EIO);
    return ret;
  }
  *seq = atoll(s);
  return op_fd;
}

int KeyValueStore::write_op_seq(int fd, uint64_t seq)
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

int KeyValueStore::mount()
{
  int ret;
  char buf[PATH_MAX];
  uint64_t initial_op_seq;

  dout(5) << "basedir " << basedir << dendl;

  // make sure global base dir exists
  if (::access(basedir.c_str(), R_OK | W_OK)) {
    ret = -errno;
    derr << "KeyValueStore::mount: unable to access basedir '" << basedir
         << "': " << cpp_strerror(ret) << dendl;
    goto done;
  }

  // get fsid
  snprintf(buf, sizeof(buf), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(buf, O_RDWR, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "KeyValueStore::mount: error opening '" << buf << "': "
	 << cpp_strerror(ret) << dendl;
    goto done;
  }

  ret = read_fsid(fsid_fd, &fsid);
  if (ret < 0) {
    derr << "KeyValueStore::mount: error reading fsid_fd: " << cpp_strerror(ret)
	 << dendl;
    goto close_fsid_fd;
  }

  if (lock_fsid() < 0) {
    derr << "KeyValueStore::mount: lock_fsid failed" << dendl;
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  dout(10) << "mount fsid is " << fsid << dendl;

  uint32_t version_stamp;
  ret = version_stamp_is_valid(&version_stamp);
  if (ret < 0) {
    derr << "KeyValueStore::mount : error in version_stamp_is_valid: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  } else if (ret == 0) {
    if (do_update) {
      derr << "KeyValueStore::mount : stale version stamp detected: "
	   << version_stamp
	   << ". Proceeding, do_update "
	   << "is set, performing disk format upgrade."
	   << dendl;
    } else {
      ret = -EINVAL;
      derr << "KeyValueStore::mount : stale version stamp " << version_stamp
	   << ". Please run the KeyValueStore update script before starting the"
	   << " OSD, or set keyvaluestore_update_to to " << target_version
	   << dendl;
      goto close_fsid_fd;
    }
  }

  // open some dir handles
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);
  if (basedir_fd < 0) {
    ret = -errno;
    derr << "KeyValueStore::mount: failed to open " << basedir << ": "
	 << cpp_strerror(ret) << dendl;
    basedir_fd = -1;
    goto close_fsid_fd;
  }

  initial_op_seq = 0;

  current_fd = ::open(current_fn.c_str(), O_RDONLY);
  if (current_fd < 0) {
    ret = -errno;
    derr << "KeyValueStore::mount: error opening: " << current_fn << ": " << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
  }

  assert(current_fd >= 0);

  op_fd = read_op_seq(&initial_op_seq);
  if (op_fd < 0) {
    derr << "KeyValueStore::mount: read_op_seq failed" << dendl;
    goto close_current_fd;
  }

  dout(5) << "mount op_seq is " << initial_op_seq << dendl;
  if (initial_op_seq == 0) {
    derr << "mount initial op seq is 0; something is wrong" << dendl;
    ret = -EINVAL;
    goto close_current_fd;
  }

  if (_detect_backend()) {
    derr << "KeyValueStore::mount error in _detect_backend" << dendl;
    ret = -1;
    goto close_current_fd;
  }

  {
    KeyValueDB *store;
    if (kv_type == KV_TYPE_LEVELDB) {
      store = new LevelDBStore(g_ceph_context, current_fn);
    } else {
      derr << "KeyValueStore::mount error: unknown backend type" << kv_type << dendl;
      ret = -1;
      goto close_current_fd;
    }

    stringstream err;
    if (store->init(err, true)) {
      delete store;
      derr << "Error initializing keyvaluestore backend: " << err.str() << dendl;
      ret = -1;
      goto close_current_fd;
    }
    backend.reset(store);
  }

  {
    LevelDBStore *omap_store = new LevelDBStore(g_ceph_context, omap_dir);

    stringstream err;
    if (omap_store->create_and_open(err)) {
      delete omap_store;
      derr << "Error initializing leveldb: " << err.str() << dendl;
      ret = -1;
      goto close_current_fd;
    }

    if (g_conf->osd_compact_leveldb_on_mount) {
      derr << "Compacting store..." << dendl;
      omap_store->compact();
      derr << "...finished compacting store" << dendl;
    }

    DBObjectMap *dbomap = new DBObjectMap(omap_store);
    ret = dbomap->init(do_update);
    if (ret < 0) {
      delete dbomap;
      derr << "Error initializing DBObjectMap: " << ret << dendl;
      goto close_current_fd;
    }
    stringstream err2;

    if (g_conf->filestore_debug_omap_check && !dbomap->check(err2)) {
      derr << err2.str() << dendl;;
      delete dbomap;
      ret = -EINVAL;
      goto close_current_fd;
    }
    object_map.reset(dbomap);
  }

  op_tp.start();
  op_finisher.start();
  ondisk_finisher.start();

  // all okay.
  return 0;

close_current_fd:
  TEMP_FAILURE_RETRY(::close(current_fd));
  current_fd = -1;
close_basedir_fd:
  TEMP_FAILURE_RETRY(::close(basedir_fd));
  basedir_fd = -1;
close_fsid_fd:
  TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
done:
  assert(!m_filestore_fail_eio || ret != -EIO);
  return ret;
}

int KeyValueStore::umount()
{
  dout(5) << "umount " << basedir << dendl;

  op_tp.stop();
  op_finisher.stop();
  ondisk_finisher.stop();

  if (fsid_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(fsid_fd));
    fsid_fd = -1;
  }
  if (op_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(op_fd));
    op_fd = -1;
  }
  if (current_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(current_fd));
    current_fd = -1;
  }
  if (basedir_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(basedir_fd));
    basedir_fd = -1;
  }

  object_map.reset();

  // nothing
  return 0;
}

int KeyValueStore::get_max_object_name_length()
{
  lock.Lock();
  int ret = pathconf(basedir.c_str(), _PC_NAME_MAX);
  if (ret < 0) {
    int err = errno;
    lock.Unlock();
    if (err == 0)
      return -EDOM;
    return -err;
  }
  lock.Unlock();
  return ret;
}


/// -----------------------------

KeyValueStore::Op *KeyValueStore::build_op(list<Transaction*>& tls,
        Context *ondisk, Context *onreadable, Context *onreadable_sync,
        TrackedOpRef osd_op)
{
  uint64_t bytes = 0, ops = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       ++p) {
    bytes += (*p)->get_num_bytes();
    ops += (*p)->get_num_ops();
  }

  Op *o = new Op;
  o->start = ceph_clock_now(g_ceph_context);
  o->tls.swap(tls);
  o->ondisk = ondisk;
  o->onreadable = onreadable;
  o->onreadable_sync = onreadable_sync;
  o->ops = ops;
  o->bytes = bytes;
  o->osd_op = osd_op;
  return o;
}

void KeyValueStore::queue_op(OpSequencer *osr, Op *o)
{
  // queue op on sequencer, then queue sequencer for the threadpool,
  // so that regardless of which order the threads pick up the
  // sequencer, the op order will be preserved.

  osr->queue(o);

  dout(5) << "queue_op " << o << " seq " << o->op << " " << *osr << " "
          << o->bytes << " bytes" << "   (queue has " << op_queue_len
          << " ops and " << op_queue_bytes << " bytes)" << dendl;
  op_wq.queue(osr);
}

void KeyValueStore::_do_op(OpSequencer *osr, ThreadPool::TPHandle &handle)
{
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
  dout(5) << "_do_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << " start" << dendl;
  int r = _do_transactions(o->tls, o->op, &handle);
  dout(10) << "_do_op " << o << " seq " << o->op << " r = " << r
           << ", finisher " << o->onreadable << " " << o->onreadable_sync << dendl;

  if (o->ondisk) {
    if (r < 0) {
      delete o->ondisk;
      o->ondisk = 0;
    } else {
      ondisk_finisher.queue(o->ondisk, r);
    }
  }
}

void KeyValueStore::_finish_op(OpSequencer *osr)
{
  Op *o = osr->dequeue();

  dout(10) << "_finish_op " << o << " seq " << o->op << " " << *osr << "/" << osr->parent << dendl;
  osr->apply_lock.Unlock();  // locked in _do_op

  utime_t lat = ceph_clock_now(g_ceph_context);
  lat -= o->start;

  if (o->onreadable_sync) {
    o->onreadable_sync->complete(0);
  }
  op_finisher.queue(o->onreadable);
  delete o;
}

int KeyValueStore::queue_transactions(Sequencer *posr, list<Transaction*> &tls,
				      TrackedOpRef osd_op)
{
  Context *onreadable;
  Context *ondisk;
  Context *onreadable_sync;
  ObjectStore::Transaction::collect_contexts(
    tls, &onreadable, &ondisk, &onreadable_sync);

  // set up the sequencer
  OpSequencer *osr;
  if (!posr)
    posr = &default_osr;
  if (posr->p) {
    osr = static_cast<OpSequencer *>(posr->p);
    dout(5) << "queue_transactions existing " << *osr << "/" << osr->parent
            << dendl; //<< " w/ q " << osr->q << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = osr;
    dout(5) << "queue_transactions new " << *osr << "/" << osr->parent << dendl;
  }

  Op *o = build_op(tls, ondisk, onreadable, onreadable_sync, osd_op);
  uint64_t op = submit_manager.op_submit_start();
  o->op = op;
  dout(5) << "queue_transactions (trailing journal) " << op << " "
          << tls <<dendl;
  queue_op(osr, o);

  submit_manager.op_submit_finish(op);

  return 0;
}

int KeyValueStore::_do_transactions(list<Transaction*> &tls, uint64_t op_seq,
  ThreadPool::TPHandle *handle)
{
  int r = 0;

  uint64_t bytes = 0, ops = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       ++p) {
    bytes += (*p)->get_num_bytes();
    ops += (*p)->get_num_ops();
  }

  int trans_num = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       ++p, trans_num++) {
    r = _do_transaction(**p, op_seq, trans_num, handle);
    if (r < 0)
      break;
    if (handle)
      handle->reset_tp_timeout();
  }

  return r;
}

unsigned KeyValueStore::_do_transaction(Transaction& t, uint64_t op_seq,
  int trans_num, ThreadPool::TPHandle *handle)
{
  dout(10) << "_do_transaction on " << &t << dendl;

  Transaction::iterator i = t.begin();

  SequencerPosition spos(op_seq, trans_num, 0);
  while (i.have_op()) {
    if (handle)
      handle->reset_tp_timeout();

    int op = i.get_op();
    int r = 0;

    switch (op) {
    case Transaction::OP_NOP:
      break;

    case Transaction::OP_TOUCH:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	r = _touch(cid, oid);
      }
      break;

    case Transaction::OP_WRITE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	bool replica = i.get_replica();
	bufferlist bl;
	i.get_bl(bl);
	r = _write(cid, oid, off, len, bl, replica);
      }
      break;

    case Transaction::OP_ZERO:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	r = _zero(cid, oid, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
	i.get_cid();
	i.get_oid();
	i.get_length();
	i.get_length();
	// deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	uint64_t off = i.get_length();
	r = _truncate(cid, oid, off);
      }
      break;

    case Transaction::OP_REMOVE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	r = _remove(cid, oid);
      }
      break;

    case Transaction::OP_SETATTR:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	string name = i.get_attrname();
	bufferlist bl;
	i.get_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(cid, oid, to_set, spos);
	if (r == -ENOSPC)
	  dout(0) << " ENOSPC on setxattr on " << cid << "/" << oid
	          << " name " << name << " size " << bl.length() << dendl;
      }
      break;

    case Transaction::OP_SETATTRS:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	map<string, bufferptr> aset;
	i.get_attrset(aset);
	r = _setattrs(cid, oid, aset, spos);
  	if (r == -ENOSPC)
	  dout(0) << " ENOSPC on setxattrs on " << cid << "/" << oid << dendl;
      }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	string name = i.get_attrname();
	r = _rmattr(cid, oid, name.c_str(), spos);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	r = _rmattrs(cid, oid, spos);
      }
      break;

    case Transaction::OP_CLONE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	ghobject_t noid = i.get_oid();
	r = _clone(cid, oid, noid, spos);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	ghobject_t noid = i.get_oid();
 	uint64_t off = i.get_length();
	uint64_t len = i.get_length();
	r = _clone_range(cid, oid, noid, off, len, off);
      }
      break;

    case Transaction::OP_CLONERANGE2:
      {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	ghobject_t noid = i.get_oid();
 	uint64_t srcoff = i.get_length();
	uint64_t len = i.get_length();
 	uint64_t dstoff = i.get_length();
	r = _clone_range(cid, oid, noid, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = i.get_cid();
	r = _create_collection(cid, spos);
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = i.get_cid();
	r = _destroy_collection(cid);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ncid = i.get_cid();
	coll_t ocid = i.get_cid();
	ghobject_t oid = i.get_oid();
	r = _collection_add(ncid, ocid, oid);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = i.get_cid();
	ghobject_t oid = i.get_oid();
	r = _remove(cid, oid);
       }
      break;

    case Transaction::OP_COLL_MOVE:
      {
	// WARNING: this is deprecated and buggy; only here to replay old journals.
	coll_t ocid = i.get_cid();
	coll_t ncid = i.get_cid();
	ghobject_t oid = i.get_oid();
	r = _collection_add(ocid, ncid, oid);
	if (r == 0)
	  r = _remove(ocid, oid);
      }
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	coll_t oldcid = i.get_cid();
	ghobject_t oldoid = i.get_oid();
	coll_t newcid = i.get_cid();
	ghobject_t newoid = i.get_oid();
	r = _collection_move_rename(oldcid, oldoid, newcid, newoid, spos);
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = i.get_cid();
	string name = i.get_attrname();
	bufferlist bl;
	i.get_bl(bl);
	r = _collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = i.get_cid();
	string name = i.get_attrname();
	r = _collection_rmattr(cid, name.c_str());
      }
      break;

    case Transaction::OP_STARTSYNC:
      _start_sync();
      break;

    case Transaction::OP_COLL_RENAME:
      {
	coll_t cid(i.get_cid());
	coll_t ncid(i.get_cid());
	r = _collection_rename(cid, ncid, spos);
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	r = _omap_clear(cid, oid, spos);
      }
      break;
    case Transaction::OP_OMAP_SETKEYS:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	map<string, bufferlist> aset;
	i.get_attrset(aset);
	r = _omap_setkeys(cid, oid, aset, spos);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	set<string> keys;
	i.get_keyset(keys);
	r = _omap_rmkeys(cid, oid, keys, spos);
      }
      break;
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	string first, last;
	first = i.get_key();
	last = i.get_key();
	r = _omap_rmkeyrange(cid, oid, first, last, spos);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
	coll_t cid(i.get_cid());
	ghobject_t oid = i.get_oid();
	bufferlist bl;
	i.get_bl(bl);
	r = _omap_setheader(cid, oid, bl, spos);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION:
      {
	coll_t cid(i.get_cid());
	uint32_t bits(i.get_u32());
	uint32_t rem(i.get_u32());
	coll_t dest(i.get_cid());
	r = _split_collection_create(cid, bits, rem, dest, spos);
      }
      break;
    case Transaction::OP_SPLIT_COLLECTION2:
      {
	coll_t cid(i.get_cid());
	uint32_t bits(i.get_u32());
	uint32_t rem(i.get_u32());
	coll_t dest(i.get_cid());
	r = _split_collection(cid, bits, rem, dest, spos);
      }
      break;

    default:
      derr << "bad op " << op << dendl;
      assert(0);
    }

    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op == Transaction::OP_CLONERANGE ||
			    op == Transaction::OP_CLONE ||
			    op == Transaction::OP_CLONERANGE2 ||
			    op == Transaction::OP_COLL_ADD))
	// -ENOENT is normally okay
	// ...including on a replayed OP_RMCOLL with checkpoint mode
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op == Transaction::OP_CLONERANGE ||
			     op == Transaction::OP_CLONE ||
			     op == Transaction::OP_CLONERANGE2))
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
	assert(0 == "unexpected error");

	if (r == -EMFILE) {
	  dump_open_fds(g_ceph_context);
	}
      }
    }

    spos.op++;
  }

  return 0;  // FIXME count errors
}

/*********************************************/



// --------------------
// objects

int KeyValueStore::collection_stat(coll_t c, struct stat *st)
{
    char fn[PATH_MAX];
    get_cdir(c, fn, sizeof(fn));
    dout(15) << "collection_stat " << fn << dendl;
    int r = ::stat(fn, st);
    if (r < 0)
        r = -errno;
    dout(10) << "collection_stat " << fn << " = " << r << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
}

bool KeyValueStore::exists(coll_t cid, const ghobject_t& oid)
{
  dout(10) << __func__ << "collection: " << cid << " object: " << oid
           << dendl;
  string key = calculate_key(cid, oid);

  KeyValueDB::Iterator it = backend->get_iterator(OBJECT);
  it->lower_bound(key);
  if (it->valid() && it->key() == key) {
    return true;
  }

  return false;
}

int KeyValueStore::stat(coll_t cid, const ghobject_t& oid,
                        struct stat *st, bool allow_eio)
{
  dout(10) << "stat " << cid << "/" << oid << dendl;

  bufferlist bl;
  string key = calculate_key(cid, oid);
  int r = get_object(key, &bl);

  if (r < 0) {
    dout(10) << "stat " << cid << "/" << oid << "=" << r << dendl;
  }

  st->st_size = bl.length();

  return r;
}

int KeyValueStore::get(const string& prefix, const string& key, bufferlist *out)
{
  std::set<string> keys;
  std::map<string, bufferlist> outs;
  int r;

  keys.insert(key);
  r = backend->get(prefix, keys, &outs);
  if (outs.empty() || r < 0)
    return -1;
  if (out)
    out->swap(outs.begin()->second);
  return 0;
}

int KeyValueStore::setkey(const string& prefix, const string& key,
                          bufferlist& bl)
{
  int r;
  KeyValueDB::Transaction t = backend->get_transaction();
  t->set(prefix, key, bl);

  r = backend->submit_transaction(t);

  return r;
}

int KeyValueStore::rmkey(const string& prefix, const string& key)
{
  KeyValueDB::Transaction t = backend->get_transaction();
  t->rmkey(prefix, key);

  return backend->submit_transaction(t);
}

int KeyValueStore::read(
  coll_t cid,
  const ghobject_t& oid,
  uint64_t offset,
  size_t len,
  bufferlist& bl,
  bool allow_eio)
{
  dout(15) << "read " << cid << "/" << oid << " " << offset << "~" << len
           << dendl;

  int r;
  bufferlist full;
  string key;

  key = calculate_key(cid, oid);
  r = get_object(key, &full);
  if (r < 0) {
    r = -ENOENT;
    dout(10) << "KeyValueStore::read(" << cid << "/" << oid << ")"
             << " doesn't exists "<< dendl;
    return r;
  }

  if (offset > full.length()) {
    r = -EINVAL;
    dout(10) << "KeyValueStore::read(" << cid << "/" << oid << ")"
             << " offset exceed the lenght of bl"<< dendl;
    return r;
  }

  if (len == 0) {
    len = full.length();
  } else if (len + offset > full.length()) {
    len = full.length() - offset;
  }

  if (len == full.length())
    bl.swap(full);
  else
    bl.substr_of(full, offset, len);

  dout(10) << "KeyValueStore::read " << cid << "/" << oid << " " << offset << "~"
	   << bl.length() << "/" << len << dendl;

  return bl.length();
}

int KeyValueStore::fiemap(coll_t cid, const ghobject_t& oid,
                    uint64_t offset, size_t len,
                    bufferlist& bl)
{
  map<uint64_t, uint64_t> m;
  m[offset] = len;
  ::encode(m, bl);
  return 0;
}

int KeyValueStore::_remove(coll_t cid, const ghobject_t& oid)
{
  dout(15) << "remove " << cid << "/" << oid << dendl;

  int r;
  string key;

  key = calculate_key(cid, oid);

  // remove
  r = rm_object(key);

  if (r < 0) {
    r = -ENOENT;
    return r;
  }

  dout(10) << "remove " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_truncate(coll_t cid, const ghobject_t& oid, uint64_t size)
{
  dout(15) << "truncate " << cid << "/" << oid << " size " << size << dendl;

  int r;
  uint64_t old_length;
  bufferlist old, curr;
  string key;

  key = calculate_key(cid, oid);
  r = get_object(key, &old);
  if (r < 0) {
    r = -ENOENT;
    return r;
  }

  old_length = old.length();

  if (old_length < size) {
    curr.swap(old);
    curr.append_zero(size-old_length);
  } else {
    old.copy(0, size, curr);
  }

  // write
  r = set_object(key, curr);

  if (r == 0)
    r = curr.length();

  dout(10) << "truncate " << cid << "/" << oid << " size " << size << " = " << r << dendl;
  return r;
}

int KeyValueStore::_touch(coll_t cid, const ghobject_t& oid)
{
  dout(15) << "touch " << cid << "/" << oid << dendl;

  int r;
  bufferlist old, curr;
  string key;

  key = calculate_key(cid, oid);
  r = get_object(key, &old);
  if (r < 0) {
    r = set_object(key, curr);
  }

  dout(10) << "touch " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_write(coll_t cid, const ghobject_t& oid,
                     uint64_t offset, size_t len,
                     const bufferlist& bl, bool replica)
{
  dout(15) << "write " << cid << "/" << oid << " " << offset << "~" << len << dendl;
  int r;
  uint64_t old_length;
  bufferlist old, cur;
  string key;

  key = calculate_key(cid, oid);
  r = get_object(key, &old);
  old_length = old.length();

  if (bl.length() < len)
      len = bl.length();

  if (r < 0) {
    if (offset != 0) {
      cur.append_zero(offset);
    }
    bl.copy(0, len, cur);
  } else {
    cur.swap(old);
    if (offset + len > old_length) {
      cur.append_zero(offset+len-old_length);
    }

    cur.copy_in(offset, len, bl);
  }

  // write
  r = set_object(key, cur);

  if (r == 0)
    r = cur.length();

  dout(10) << "write " << cid << "/" << oid << " " << offset << "~" << len << " = " << r << dendl;
  return r;
}

int KeyValueStore::_zero(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len)
{
  dout(15) << "zero " << cid << "/" << oid << " " << offset << "~" << len << dendl;
  int ret = 0;

  bufferptr bp(len);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);
  ret = _write(cid, oid, offset, len, bl);

  dout(20) << "zero " << cid << "/" << oid << " " << offset << "~" << len << " = " << ret << dendl;
  return ret;
}

int KeyValueStore::_clone(coll_t cid, const ghobject_t& oldoid,
                          const ghobject_t& newoid,
                          const SequencerPosition& spos)
{
  dout(15) << "clone " << cid << "/" << oldoid << " -> " << cid << "/" << newoid << dendl;

  int r;
  bufferlist full;
  string oldkey, newkey;

  oldkey = calculate_key(cid, oldoid);
  r = get_object(oldkey, &full);
  if (r < 0) {
    r = -ENOENT;
    dout(10) << "KeyValueStore::read(" << cid << "/" << oldoid << ") get error: "
	     << cpp_strerror(r) << dendl;
    goto out;
  }

  // write
  newkey = calculate_key(cid, newoid);

  r = set_object(newkey, full);

  if (r < 0) {
    r = -ENOENT;
    goto out;
  }

  dout(20) << "objectmap clone" << dendl;
  r = object_map->clone(oldoid, newoid, &spos);
  if (r < 0 && r != -ENOENT)
    goto out;

 out:
  dout(10) << "clone " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_clone_range(coll_t cid, const ghobject_t& oldoid, const ghobject_t& newoid,
			    uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
  dout(15) << "clone_range " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " " << srcoff << "~" << len << " to " << dstoff
           << dendl;

  int r;
  bufferlist bl;

  r = read(cid, oldoid, srcoff, len, bl);
  if (r < 0)
    goto out;

  r = _write(cid, newoid, dstoff, len, bl);

 out:
  dout(10) << "clone_range " << cid << "/" << oldoid << " -> " << cid << "/"
           << newoid << " " << srcoff << "~" << len << " to " << dstoff
           << " = " << r << dendl;
  return r;
}

// debug EIO injection
void KeyValueStore::inject_data_error(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  dout(10) << __func__ << ": init error on " << oid << dendl;
  data_error_set.insert(oid);
}
void KeyValueStore::inject_mdata_error(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  dout(10) << __func__ << ": init error on " << oid << dendl;
  mdata_error_set.insert(oid);
}
void KeyValueStore::debug_obj_on_delete(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  dout(10) << __func__ << ": clear error on " << oid << dendl;
  data_error_set.erase(oid);
  mdata_error_set.erase(oid);
}
bool KeyValueStore::debug_data_eio(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  if (data_error_set.count(oid)) {
    dout(10) << __func__ << ": inject error on " << oid << dendl;
    return true;
  } else {
    return false;
  }
}
bool KeyValueStore::debug_mdata_eio(const ghobject_t &oid) {
  Mutex::Locker l(read_error_lock);
  if (mdata_error_set.count(oid)) {
    dout(10) << __func__ << ": inject error on " << oid << dendl;
    return true;
  } else {
    return false;
  }
}


// objects

int KeyValueStore::getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr &bp)
{
  dout(15) << "getattr " << cid << "/" << oid << " '" << name << "'" << dendl;

  int r;
  map<string, bufferlist> got;
  set<string> to_get;

  to_get.insert(string(name));
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
  r = 0;

 out:
  dout(10) << "getattr " << cid << "/" << oid << " '" << name << "' = " << r
           << dendl;
  return r;
}

int KeyValueStore::getattrs(coll_t cid, const ghobject_t& oid,
                           map<string,bufferptr>& aset, bool user_only)
{
  int r;
  set<string> omap_attrs;
  map<string, bufferlist> omap_aset;

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
  assert(omap_attrs.size() == omap_aset.size());
  for (map<string, bufferlist>::iterator i = omap_aset.begin();
          i != omap_aset.end(); ++i) {
    string key;
    if (user_only) {
      if (i->first[0] != SEP_C)
        continue;
      if (i->first == SEP_S)
        continue;
      key = i->first.substr(1, i->first.size());
    } else {
      key = i->first;
    }
    aset.insert(make_pair(key,
                bufferptr(i->second.c_str(), i->second.length())));
  }
 out:
  dout(10) << "getattrs " << cid << "/" << oid << " = " << r << dendl;

  return r;
}

int KeyValueStore::_setattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset,
			 const SequencerPosition &spos)
{
  dout(15) << "setattrs " << cid << "/" << oid << dendl;

  int r;
  map<string, bufferlist> attrs;
  for (map<string, bufferptr>::iterator it = aset.begin(); it != aset.end(); ++it) {
      bufferlist bl;
      bl.append(it->second);
      attrs.insert(make_pair(it->first, bl));
  }

  r = object_map->set_xattrs(oid, attrs, &spos);
  if (r < 0) {
    dout(10) << __func__ << " could not set_xattrs r = " << r << dendl;
  }

  dout(10) << "setattrs " << cid << "/" << oid << " = " << r << dendl;
  return r;
}


int KeyValueStore::_rmattr(coll_t cid, const ghobject_t& oid, const char *name,
		       const SequencerPosition &spos)
{
  dout(15) << "rmattr " << cid << "/" << oid << " '" << name << "'" << dendl;

  int r;
  set<string> to_remove;

  to_remove.insert(string(name));
  r = object_map->remove_xattrs(oid, to_remove, &spos);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not remove_xattrs index r = " << r << dendl;
  }

  dout(10) << "rmattr " << cid << "/" << oid << " '" << name << "' = " << r
           << dendl;
  return r;
}

int KeyValueStore::_rmattrs(coll_t cid, const ghobject_t& oid,
			const SequencerPosition &spos)
{
  dout(15) << "rmattrs " << cid << "/" << oid << dendl;

  int r;
  set<string> omap_attrs;

  r = object_map->get_all_xattrs(oid, &omap_attrs);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not get omap_attrs r = " << r << dendl;
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  r = object_map->remove_xattrs(oid, omap_attrs, &spos);
  if (r < 0 && r != -ENOENT) {
    dout(10) << __func__ << " could not remove omap_attrs r = " << r << dendl;
    return r;
  }
  if (r == -ENOENT)
    r = 0;

  dout(10) << "rmattrs " << cid << "/" << oid << " = " << r << dendl;
  return r;
}

// KeyValueStore xattr replacement
//
// collections

int KeyValueStore::collection_getattr(coll_t c, const char *name,
                                     void *value, size_t size)
{
  dout(15) << "collection_getattr " << c.to_str() << " '" << name << "' len "
           << size << dendl;

  bufferlist bl;
  int r;

  r = collection_getattr(c, name, bl);
  if (r < 0)
      goto out;

  if (bl.length() < size) {
    r = bl.length();
    bl.copy(0, bl.length(), static_cast<char*>(value));
  } else {
    r = size;
    bl.copy(0, size, static_cast<char*>(value));
  }

out:
  dout(10) << "collection_getattr " << c.to_str() << " '" << name << "' len "
           << size << " = " << r << dendl;
  return r;
}

int KeyValueStore::collection_getattr(coll_t c, const char *name, bufferlist& bl)
{
  dout(15) << "collection_getattr " << c.to_str() << " '" << name
           << "'" << dendl;

  int r;
  string key;

  key = string(name);

  r = get(collection_attr_prefix(c), key, &bl);
  if (r < 0) {
    dout(10) << __func__ << " could not get key" << key << dendl;
    r = -EINVAL;
  }

  dout(10) << "collection_getattr " << c.to_str() << " '" << name << "' len "
           << bl.length() << " = " << r << dendl;
  return bl.length();
}

int KeyValueStore::collection_getattrs(coll_t cid, map<string,bufferptr>& aset)
{
  dout(10) << "collection_getattrs " << cid.to_str() << dendl;

  int r;
  map<string, bufferlist> out;
  set<string> keys;

  for (map<string, bufferptr>::iterator it = aset.begin(); it != aset.end();
       it++) {
      keys.insert(it->first);
  }

  r = backend->get(collection_attr_prefix(cid), keys, &out);
  if (r < 0) {
    dout(10) << __func__ << " could not get keys" << dendl;
    r = -EINVAL;
    goto out;
  }

  for (map<string, bufferlist>::iterator it = out.begin(); it != out.end();
       ++it) {
    bufferptr ptr(it->second.c_str(), it->second.length());
    aset.insert(make_pair(it->first, ptr));
  }

 out:
  dout(10) << "collection_getattrs " << cid.to_str() << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_setattr(coll_t c, const char *name,
				  const void *value, size_t size)
{
  dout(10) << "collection_setattr " << c.to_str() << " '" << name << "' len "
           << size << dendl;

  int r;
  bufferlist bl;

  bl.append(reinterpret_cast<const char*>(value), size);

  r = setkey(collection_attr_prefix(c), string(name), bl);

  if (r < 0) {
    r = -ENOENT;
  }

  dout(10) << "collection_setattr " << collection_attr_prefix(c) << " '"
           << name << "' len " << size << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_rmattr(coll_t c, const char *name)
{
  dout(15) << "collection_rmattr " << c.to_str() << dendl;

  int r = rmkey(collection_attr_prefix(c), string(name));

  if (r < 0) {
    r = -ENOENT;
  }

  dout(10) << "collection_rmattr " << collection_attr_prefix(c) << " = "
           << r << dendl;
  return r;
}

int KeyValueStore::_collection_setattrs(coll_t cid, map<string,bufferptr>& aset)
{
  dout(15) << "collection_setattrs " << cid.to_str() << dendl;

  map<string, bufferlist> attrs;

  for (map<string, bufferptr>::iterator it = aset.begin(); it != aset.end();
       ++it) {
    bufferlist bl;
    bl.append(it->second);
    attrs.insert(make_pair(it->first, bl));
  }

  KeyValueDB::Transaction t = backend->get_transaction();
  t->set(collection_attr_prefix(cid), attrs);

  int r = backend->submit_transaction(t);

  if (r < 0) {
    r = -ENOENT;
  }

  dout(10) << "collection_setattrs " << cid.to_str() << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_remove_recursive(const coll_t &cid,
					    const SequencerPosition &spos)
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
  r = 0;
  while (!max.is_max()) {
    r = collection_list_partial(cid, max, 200, 300, 0, &objects, &max);
    if (r < 0)
      return r;
    for (vector<ghobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      r = _remove(cid, *i);
      if (r < 0)
	return r;
    }
  }
  return _destroy_collection(cid);
}

int KeyValueStore::_collection_rename(const coll_t &cid, const coll_t &ncid,
				  const SequencerPosition& spos)
{
  return -EOPNOTSUPP;
}

// --------------------------
// collections

int KeyValueStore::collection_version_current(coll_t c, uint32_t *version)
{
  *version = COLLECTION_VERSION;
  if (*version == target_version)
    return 1;
  else
    return 0;
}

int KeyValueStore::list_collections(vector<coll_t>& ls)
{
  dout(10) << "list_collections" << dendl;

  KeyValueDB::Iterator iter = backend->get_iterator(COLLECTION);

  for (iter->seek_to_first(); iter->valid(); iter->next()) {
    ls.push_back(coll_t(iter->value().c_str()));
  }

  return 0;
}

bool KeyValueStore::collection_exists(coll_t c)
{
  bufferlist bl;
  return get(COLLECTION, c.to_str(), &bl) == 0;
}

bool KeyValueStore::collection_empty(coll_t c)
{
  dout(15) << "collection_empty " << c << dendl;

  coll_t coll;

  KeyValueDB::Iterator iter = backend->get_iterator(OBJECT);

  iter->lower_bound(calculate_key(c));
  if (!iter->valid()) {
    return true;
  }

  if (parse_object(iter->key(), &coll, 0)) {
    if (coll != c)
        return true;
  }

  return false;
}

int KeyValueStore::collection_list_range(coll_t c, ghobject_t start, ghobject_t end,
                                     snapid_t seq, vector<ghobject_t> *ls)
{
  bool done = false;
  ghobject_t next = start;

  while (!done) {
    vector<ghobject_t> next_objects;
    int r = collection_list_partial(c, next,
                                get_ideal_list_min(), get_ideal_list_max(),
                                seq, &next_objects, &next);
    if (r < 0)
      return r;

    ls->insert(ls->end(), next_objects.begin(), next_objects.end());

    // special case for empty collection
    if (ls->empty()) {
      break;
    }

    while (!ls->empty() && ls->back() >= end) {
      ls->pop_back();
      done = true;
    }

    if (next >= end) {
      done = true;
    }
  }

  return 0;
}

int KeyValueStore::collection_list_partial(coll_t c, ghobject_t start,
				       int min, int max, snapid_t seq,
				       vector<ghobject_t> *ls, ghobject_t *next)
{
  dout(10) << "collection_list_partial: " << c << " start:" << start
           << " is_max:" << start.is_max() << dendl;

  int r = 0;

  if (min < 0 || max < 0)
      return -EINVAL;

  if (start.is_max())
      return 0;

  // while start is default constructed
  if (start.hobj.is_min()) {
    KeyValueDB::Iterator iter = backend->get_iterator(OBJECT);
    if (!iter) {
      r = -EINVAL;
      dout(10) << "KeyValueStore::collection_list invalid collection: "
               << c << dendl;
      return r;
    }

    // Seek for the first object
    r = iter->lower_bound(calculate_key(c));
    if (r < 0) {
      dout(10) << "seek_to_first failed: prefix " << calculate_key(c) << dendl;
      return r;
    }

    r = -EINVAL;
    while (iter->valid()) {
      if (parse_object(iter->key(), 0, &start)) {
        r = 0;
        break;
      }
      dout(20) << "parse_object failed coll: " << c << dendl;
      iter->next();
    }

    if (r < 0) {
      dout(10) << "KeyValueStore::collection_list failed to find first object"
               << ": prefix " << calculate_key(c) << dendl;
      return r;
    }
  }

  ghobject_t obj;
  coll_t coll;
  KeyValueDB::Iterator iter = backend->get_iterator(OBJECT);
  int size = 0;

  for (iter->lower_bound(calculate_key(c, start)); iter->valid(); iter->next()) {
    if (!parse_object(iter->key(), &coll, &obj)) {
      dout(20) << "KeyValueStore::collection_list_partial invalid object: "
               << iter->key() << dendl;
      continue;
    }

    if (coll != c || (min != 0 && size >= min)) {
      if (next)
        *next = obj;
      break;
    }

    if (start > obj) {
      dout(10) << "KeyValueStore::collection_list_partial incorrect sort: "
               << start << " > " << obj << dendl;
      return -1;
    }

    size++;
    ls->push_back(obj);
    start = obj;
  }

  if (ls)
    dout(20) << "objects: " << *ls << dendl;

  if (coll != c || !iter->valid())
    if (next)
      *next = ghobject_t::get_max();

  return 0;
}

int KeyValueStore::collection_list(coll_t c, vector<ghobject_t>& ls)
{
  int r;
  ghobject_t start;

  r = collection_list_partial(c, start, 0, 0, 0, &ls, 0);
  return r;
}

int KeyValueStore::omap_get(coll_t c, const ghobject_t &hoid,
			bufferlist *header,
			map<string, bufferlist> *out)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  int r = object_map->get(hoid, header, out);
  if (r < 0 && r != -ENOENT) {
    return r;
  }
  return 0;
}

int KeyValueStore::omap_get_header(
  coll_t c,
  const ghobject_t &hoid,
  bufferlist *bl,
  bool allow_eio)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  int r = object_map->get_header(hoid, bl);
  if (r < 0 && r != -ENOENT) {
    assert(allow_eio || !m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int KeyValueStore::omap_get_keys(coll_t c, const ghobject_t &hoid, set<string> *keys)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  int r = object_map->get_keys(hoid, keys);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int KeyValueStore::omap_get_values(coll_t c, const ghobject_t &hoid,
			       const set<string> &keys,
			       map<string, bufferlist> *out)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  int r = object_map->get_values(hoid, keys, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

int KeyValueStore::omap_check_keys(coll_t c, const ghobject_t &hoid,
			       const set<string> &keys,
			       set<string> *out)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  int r = object_map->check_keys(hoid, keys, out);
  if (r < 0 && r != -ENOENT) {
    assert(!m_filestore_fail_eio || r != -EIO);
    return r;
  }
  return 0;
}

ObjectMap::ObjectMapIterator KeyValueStore::get_omap_iterator(coll_t c,
							  const ghobject_t &hoid)
{
  dout(15) << __func__ << " " << c << "/" << hoid << dendl;
  return object_map->get_iterator(hoid);
}

int KeyValueStore::_create_collection(coll_t c)
{
  dout(15) << "create_collection " << c.to_str() << dendl;

  int r;
  bufferlist bl;

  if (get(COLLECTION, c.to_str(), &bl) >= 0) {
    r = -EEXIST;
    goto out;
  }

  bl.append(c.to_str());
  r = setkey(COLLECTION, c.to_str(), bl);

  if (r < 0) {
    r = -EINVAL;
  }

out:
  dout(10) << "create_collection " << c.to_str() << " = " << r << dendl;

  return r;
}

int KeyValueStore::_destroy_collection(coll_t c)
{
  dout(15) << "_destroy_collection " << c.to_str() << dendl;

  int r;

  if (!collection_empty(c)) {
    r = -EINVAL;
    goto out;
  }

  r = rmkey(COLLECTION, c.to_str());

  if (r < 0) {
    r = -ENOENT;
  }

out:
  dout(10) << "_destroy_collection " << c.to_str() << " = " << r << dendl;
  return r;
}


int KeyValueStore::_collection_add(coll_t c, coll_t oldcid, const ghobject_t& o)
{
  dout(15) << "collection_add " << c << "/" << o << " from " << oldcid << "/"
           << o << dendl;

  bufferlist bl;
  int r = read(oldcid, o, 0, 0, bl);
  if (r < 0) {
    r = -EINVAL;
    return r;
  }

  r = _write(c, o, 0, bl.length(), bl);
  if (r < 0) {
    r = -EINVAL;
    return r;
  }

  dout(10) << "collection_add " << c << "/" << o << " from " << oldcid << "/"
           << o << " = " << r << dendl;
  return r;
}

int KeyValueStore::_collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
				       coll_t c, const ghobject_t& o,
				       const SequencerPosition& spos)
{
  dout(15) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
           << oldoid << dendl;
  int r = 0;

  {
    bufferlist bl;
    r = read(oldcid, oldoid, 0, 0, bl);
    if (r < 0) {
      r = -EINVAL;
      return r;
    }

    r = _write(c, o, 0, bl.length(), bl);
    if (r < 0) {
      r = -EINVAL;
      return r;
    }

    // the name changed; link the omap content
    r = object_map->clone(oldoid, o, &spos);
    if (r == -ENOENT)
      r = 0;
  }
  _remove(oldcid, oldoid);

  dout(10) << __func__ << " " << c << "/" << o << " from " << oldcid << "/"
           << oldoid << " = " << r << dendl;
  return r;
}

int KeyValueStore::_omap_clear(coll_t cid, const ghobject_t &hoid,
			   const SequencerPosition &spos) {
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;
  int r = object_map->clear(hoid, &spos);
  if (r < 0 && r != -ENOENT)
    return r;
  return 0;
}

int KeyValueStore::_omap_setkeys(coll_t cid, const ghobject_t &hoid,
			     const map<string, bufferlist> &aset,
			     const SequencerPosition &spos) {
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;
  return object_map->set_keys(hoid, aset, &spos);
}

int KeyValueStore::_omap_rmkeys(coll_t cid, const ghobject_t &hoid,
			    const set<string> &keys,
			    const SequencerPosition &spos) {
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;
  int r = object_map->rm_keys(hoid, keys, &spos);
  if (r < 0 && r != -ENOENT)
    return r;
  return 0;
}

int KeyValueStore::_omap_rmkeyrange(coll_t cid, const ghobject_t &hoid,
				const string& first, const string& last,
				const SequencerPosition &spos) {
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

int KeyValueStore::_omap_setheader(coll_t cid, const ghobject_t &hoid,
			       const bufferlist &bl,
			       const SequencerPosition &spos)
{
  dout(15) << __func__ << " " << cid << "/" << hoid << dendl;
  return object_map->set_header(hoid, bl, &spos);
}

int KeyValueStore::_split_collection(coll_t cid,
				 uint32_t bits,
				 uint32_t rem,
				 coll_t dest,
				 const SequencerPosition &spos)
{
  int r;
  {
    dout(15) << __func__ << " " << cid << " bits: " << bits << dendl;
    if (!collection_exists(cid)) {
      dout(2) << __func__ << ": " << cid << " DNE" << dendl;
      return 0;
    }
    if (!collection_exists(dest)) {
      dout(2) << __func__ << ": " << dest << " DNE" << dendl;
      return 0;
    }

    vector<ghobject_t> objects;
    int move_size = 0;
    r = collection_list(cid, objects);
    dout(20) << __func__ << cid << "objects size: " << objects.size()
             << dendl;

    for (vector<ghobject_t>::iterator i = objects.begin();
         i != objects.end(); ++i) {
      if (i->match(bits, rem)) {
        if (_collection_add(dest, cid, *i) < 0) {
          return -1;
        }
        _remove(cid, *i);
        move_size++;
      }
    }
    dout(20) << __func__ << "move" << move_size << " object from " << cid
             << "to " << dest << dendl;
  }

  if (g_conf->filestore_debug_verify_split) {
    vector<ghobject_t> objects;
    ghobject_t next;
    while (1) {
      collection_list_partial(
	cid,
	next,
	get_ideal_list_min(), get_ideal_list_max(), 0,
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
      collection_list_partial(
	dest,
	next,
	get_ideal_list_min(), get_ideal_list_max(), 0,
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
  return r;
}

const char** KeyValueStore::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    "filestore_min_sync_interval",
    "filestore_max_sync_interval",
    "filestore_queue_max_ops",
    "filestore_queue_max_bytes",
    "filestore_queue_committing_max_ops",
    "filestore_queue_committing_max_bytes",
    "filestore_commit_timeout",
    "filestore_dump_file",
    "filestore_kill_at",
    "filestore_fail_eio",
    "filestore_replica_fadvise",
    "filestore_sloppy_crc",
    "filestore_sloppy_crc_block_size",
    NULL
  };
  return KEYS;
}

void KeyValueStore::handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed)
{
}

void KeyValueStore::dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq, OpSequencer *osr)
{
}
