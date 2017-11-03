// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_ZFSCSTOREBACKEND_H
#define CEPH_ZFSCSTOREBACKEND_H

#ifdef HAVE_LIBZFS
#include "GenericCStoreBackend.h"
#include "os/fs/ZFS.h"

class ZFSCStoreBackend : public GenericCStoreBackend {
private:
  ZFS zfs;
  ZFS::Handle *base_zh;
  ZFS::Handle *current_zh;
  bool m_filestore_zfs_snap;
  int update_current_zh();
public:
  explicit ZFSCStoreBackend(CStore *fs);
  ~ZFSCStoreBackend();
  int detect_features();
  bool can_checkpoint();
  int create_current();
  int list_checkpoints(list<string>& ls);
  int create_checkpoint(const string& name, uint64_t *cid);
  int rollback_to(const string& name);
  int destroy_checkpoint(const string& name);
};
#endif
#endif
