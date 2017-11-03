// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_XFSCSTOREBACKEND_H
#define CEPH_XFSCSTOREBACKEND_H

#include "GenericCStoreBackend.h"

#include "include/int_types.h"

class XfsCStoreBackend : public GenericCStoreBackend {
private:
  bool m_has_extsize;
  int set_extsize(int fd, unsigned int val);
public:
  explicit XfsCStoreBackend(CStore *fs);
  ~XfsCStoreBackend() {}
  const char *get_name() {
    return "xfs";
  }
  int detect_features();
  int set_alloc_hint(int fd, uint64_t hint);
};

#endif /* CEPH_XFSFILESTOREBACKEND_H */
