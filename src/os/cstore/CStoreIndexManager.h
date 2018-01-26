// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef OS_CSTORE_INDEXMANAGER_H
#define OS_CSTORE_INDEXMANAGER_H

#include "include/memory.h"
#include "include/unordered_map.h"

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/config.h"
#include "common/debug.h"

#include "CStoreCollectionIndex.h"
#include "CStoreHashIndex.h"


/// Public type for Index
struct CStoreIndex {
  CStoreCollectionIndex *index;

  CStoreIndex() : index(NULL) {}
  explicit CStoreIndex(CStoreCollectionIndex* index) : index(index) {}

  CStoreCollectionIndex *operator->() { return index; }
  CStoreCollectionIndex &operator*() { return *index; }
};


/**
 * Encapsulates mutual exclusion for CStoreCollectionIndexes.
 *
 * Allowing a modification (removal or addition of an object) to occur
 * while a read is occuring (lookup of an object's path and use of
 * that path) may result in the path becoming invalid.  Thus, during
 * the lifetime of a CStoreCollectionIndex object and any paths returned
 * by it, no other concurrent accesses may be allowed.
 * This is enforced by using CStoreCollectionIndex::access_lock
 */
class CStoreIndexManager {
  Mutex lock; ///< Lock for Index Manager
  bool upgrade;
  ceph::unordered_map<coll_t, CStoreCollectionIndex* > col_indices;

  /**
   * Index factory
   *
   * Encapsulates logic for handling legacy FileStore
   * layouts
   *
   * @param [in] c Collection for which to get index
   * @param [in] path Path to collection
   * @param [out] index Index for c
   * @return error code
   */
  int build_index(coll_t c, const char *path, CStoreCollectionIndex **index);
public:
  /// Constructor
  explicit CStoreIndexManager(bool upgrade) : lock("CStoreIndexManager lock"),
		    		        upgrade(upgrade) {}

  ~CStoreIndexManager();

  /**
   * Reserve and return index for c
   *
   * @param [in] c Collection for which to get index
   * @param [in] baseDir base directory of collections
   * @param [out] index Index for c
   * @return error code
   */
  int get_index(coll_t c, const string& baseDir, CStoreIndex *index);

  /**
   * Initialize index for collection c at path
   *
   * @param [in] c Collection for which to init Index
   * @param [in] path Path to collection
   * @param [in] filestore_version version of containing FileStore
   * @return error code
   */
  int init_index(coll_t c, const char *path, uint32_t filestore_version);
};

#endif
