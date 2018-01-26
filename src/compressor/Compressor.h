// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMPRESSOR_H
#define CEPH_COMPRESSOR_H

#include "include/int_types.h"
#include "include/Context.h"

class Compressor;
typedef shared_ptr<Compressor> CompressorRef;


class Compressor {
 public:
  enum CompressionAlgorithm {
    COMP_ALG_NONE = 0,
    COMP_ALG_SNAPPY = 1,
    COMP_ALG_ZLIB = 2,
    COMP_ALG_LAST	//the last value for range checks
  };

  static const char * get_comp_alg_name(int a);
  static boost::optional<CompressionAlgorithm> get_comp_alg_type(const std::string &s);

  virtual ~Compressor() {}
  virtual int compress(const bufferlist &in, bufferlist &out) = 0;
  virtual int decompress(const bufferlist &in, bufferlist &out) = 0;

  static CompressorRef create(CephContext *cct, const string &type);
};



#endif
