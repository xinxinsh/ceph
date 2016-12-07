// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2016 Chinac <shuxinxin@chinac.com>
 *
 * Author: xinxin shu <shuxinxin@chinac.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef CEPH_ERASURE_CODE_MLXJERASURE_H
#define CEPH_ERASURE_CODE_MLXJERASURE_H

#include "erasure-code/ErasureCode.h"
#include "include/buffer_fwd.h"

#define DEFAULT_RULESET_ROOT "default"
#define DEFAULT_RULESET_FAILURE_DOMAIN "host"

class ErasureCodeMlxJerasure : public ErasureCode {
public:
  const unsigned MLX_ALIGNMENT_SIZE = 64; 
  static int tcount;
  int k;
  std::string DEFAULT_K;
  int m;
  std::string DEFAULT_M;
  int w;
  std::string DEFAULT_W;
  string ruleset_root;
  string ruleset_failure_domain;
  const char* nic;
  struct mlx_ec_context *ctx;
  
  explicit ErasureCodeMlxJerasure(const char* _nic) :
    k(0),
    DEFAULT_K("2"),
    m(0),
    DEFAULT_M("1"),
    w(0),
    DEFAULT_W("4"),
    ruleset_root(DEFAULT_RULESET_ROOT),
    ruleset_failure_domain(DEFAULT_RULESET_FAILURE_DOMAIN),
    nic(_nic) {}

  virtual ~ErasureCodeMlxJerasure(); 
  
  virtual int create_ruleset(const string &name,
                             CrushWrapper &crush,
                             ostream *ss) const;

  virtual unsigned int get_chunk_count() const 
  {
    return k + m;
  }
  
  virtual unsigned int get_data_chunk_count() const
  {
    return k;
  }
  
  virtual const char* get_nic() const 
  {
    return nic;
  }

  virtual unsigned int get_chunk_size(unsigned int obj_size) const;

  virtual int encode_chunks(const set<int>& want_to_read,
                            map<int, bufferlist> *encoded);

  virtual int decode_chunks(const set<int>& want_to_read,
                            const map<int, bufferlist>& chunks,
                            map<int, bufferlist> *decoded);

  virtual int init(ErasureCodeProfile &profile, ostream *ss);

  virtual int jerasure_encode(char **data, char **coding, int blocksize);
  virtual int jerasure_decode(int *erasure, char **data, char **coding, int blocksize);

  virtual void prepare();

  unsigned get_alignment() const;

  int sanity_check_ec_capability(const char* name);
protected:
  virtual int parse(ErasureCodeProfile &profile, ostream *ss); 
};

#endif
