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

#ifndef OS_CSTORE_KEYVALUESTORE_H
#define OS_CSTORE_KEYVALUESTORE_H

#include <string>
#include <vector>
#include "include/memory.h"
#include "kv/KeyValueDB.h"
#include "common/hobject.h"
#include "os/ObjectMap.h"

class CStoreSequencerPosition;

/**
 * Encapsulates the FileStore key value store
 *
 * Implementations of this interface will be used to implement TMAP
 */
class CStoreObjectMap {
public:
  /// Set keys and values from specified map
  virtual int set_keys(
    const ghobject_t &oid,              ///< [in] object containing map
    const map<string, bufferlist> &set,  ///< [in] key to value map to set
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Set header
  virtual int set_header(
    const ghobject_t &oid,              ///< [in] object containing map
    const bufferlist &bl,               ///< [in] header to set
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Retrieve header
  virtual int get_header(
    const ghobject_t &oid,              ///< [in] object containing map
    bufferlist *bl                      ///< [out] header to set
    ) = 0;

  /// Clear all map keys and values from oid
  virtual int clear(
    const ghobject_t &oid,             ///< [in] object containing map
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Clear all map keys and values in to_clear from oid
  virtual int rm_keys(
    const ghobject_t &oid,              ///< [in] object containing map
    const set<string> &to_clear,        ///< [in] Keys to clear
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// remove keys by prefix
  virtual int rm_keys_by_prefix(
    const string &prefix,
    const set<string>& to_rm,
    const CStoreSequencerPosition *spos=0
    ) = 0;
  /// set keys by prefix
  virtual int set_keys_by_prefix(
    const string &prefix,
    const map<string, bufferlist> &sets,
    const CStoreSequencerPosition *spos=0
    ) = 0;

  /// Clear all omap keys and the header
  virtual int clear_keys_header(
    const ghobject_t &oid,              ///< [in] oid to clear
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// Get all keys and values
  virtual int get(
    const ghobject_t &oid,             ///< [in] object containing map
    bufferlist *header,                ///< [out] Returned Header
    map<string, bufferlist> *out       ///< [out] Returned keys and values
    ) = 0;

  /// Get values for supplied keys
  virtual int get_keys(
    const ghobject_t &oid,             ///< [in] object containing map
    set<string> *keys                  ///< [out] Keys defined on oid
    ) = 0;

  /// Get values for supplied keys
  virtual int get_values(
    const ghobject_t &oid,             ///< [in] object containing map
    const set<string> &keys,           ///< [in] Keys to get
    map<string, bufferlist> *out       ///< [out] Returned keys and values
    ) = 0;

  /// Check key existence
  virtual int check_keys(
    const ghobject_t &oid,             ///< [in] object containing map
    const set<string> &keys,           ///< [in] Keys to check
    set<string> *out                   ///< [out] Subset of keys defined on oid
    ) = 0;

  /// Get xattrs
  virtual int get_xattrs(
    const ghobject_t &oid,             ///< [in] object
    const set<string> &to_get,         ///< [in] keys to get
    map<string, bufferlist> *out       ///< [out] subset of attrs/vals defined
    ) = 0;

  /// Get all xattrs
  virtual int get_all_xattrs(
    const ghobject_t &oid,             ///< [in] object
    set<string> *out                   ///< [out] attrs and values
    ) = 0;

  /// set xattrs in to_set
  virtual int set_xattrs(
    const ghobject_t &oid,                ///< [in] object
    const map<string, bufferlist> &to_set,///< [in] attrs/values to set
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;

  /// remove xattrs in to_remove
  virtual int remove_xattrs(
    const ghobject_t &oid,               ///< [in] object
    const set<string> &to_remove,        ///< [in] attrs to remove
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) = 0;


  /// Clone keys efficiently from oid map to target map
  virtual int clone(
    const ghobject_t &oid,             ///< [in] object containing map
    const ghobject_t &target,           ///< [in] target of clone
    const CStoreSequencerPosition *spos=0     ///< [in] sequencer position
    ) { return 0; }

  /// Ensure all previous writes are durable
  virtual int sync(
    const ghobject_t *oid=0,          ///< [in] object
    const CStoreSequencerPosition *spos=0   ///< [in] Sequencer
    ) { return 0; }

  virtual bool check(std::ostream &out) { return true; }

  class CStoreObjectMapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  public:
    virtual int seek_to_first() override {return 0;}
    virtual int upper_bound(const string &after) override {return 0;}
    virtual int lower_bound(const string &to) override {return 0;}
    virtual bool valid() override {return false;}
    virtual int next(bool validate=true) override {return 0;}
    virtual string key() override {assert(0); return "";}
    virtual bufferlist value() override {assert(0); return bufferlist();}
    virtual int status() override {return 0;}
  };

  typedef ceph::shared_ptr<CStoreObjectMapIteratorImpl> CStoreObjectMapIterator;
  virtual CStoreObjectMapIterator get_iterator(const ghobject_t &oid) {
    return CStoreObjectMapIterator();
  }
  virtual CStoreObjectMapIterator get_whole_iterator(const string &prefix) {
    return CStoreObjectMapIterator();
  }


  virtual ~CStoreObjectMap() {}
};

#endif
