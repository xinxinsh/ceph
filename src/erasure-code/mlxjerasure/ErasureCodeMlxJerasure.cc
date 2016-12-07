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

#include "crush/CrushWrapper.h"
#include "osd/osd_types.h"
#include "common/debug.h"
#include "ErasureCodeMlxJerasure.h"
#include "ErasureCode_MlxJerasure_common.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h> 

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeMlxJerasure: ";
}

int ErasureCodeMlxJerasure::tcount = 1;
ErasureCodeMlxJerasure::~ErasureCodeMlxJerasure() {
  //std::ostringstream ss;
  //free_mlx_ec_context(ctx, &ss);
}

int ErasureCodeMlxJerasure::create_ruleset(const string &name,
					CrushWrapper &crush,
					ostream *ss) const
{
  int ruleid = crush.add_simple_ruleset(name, ruleset_root, ruleset_failure_domain,
					"indep", pg_pool_t::TYPE_ERASURE, ss);
  if (ruleid < 0)
    return ruleid;
  else {
    crush.set_rule_mask_max_size(ruleid, get_chunk_count());
    return crush.get_rule_mask_ruleset(ruleid);
  }
}

int ErasureCodeMlxJerasure::init(ErasureCodeProfile& profile, ostream *ss)
{
  int err = 0;
  
  err |= to_string("ruleset-root", profile,
		   &ruleset_root,
		   DEFAULT_RULESET_ROOT, ss);
  err |= to_string("ruleset-failure-domain", profile,
		   &ruleset_failure_domain,
		   DEFAULT_RULESET_FAILURE_DOMAIN, ss);
  err |= parse(profile, ss);
  if (err)
    return err;

  prepare();
  ErasureCode::init(profile, ss);
  return err;
}

void ErasureCodeMlxJerasure::prepare()
{
  std::ostringstream ss;

  ctx = alloc_mlx_ec_context(nic, 1, k, m, w, &ss);
  if (!ctx) {
    derr << __func__ << " failed to allc ec context " << ss.str() << dendl;
  }
}

int ErasureCodeMlxJerasure::sanity_check_ec_capability(const char* name)
{
  std::ostringstream ss;

  int r = check_ec_capability(name, &ss);
  if (r)
    derr << __func__ << " NIC " << name << " does not support EC " << dendl;
  return r;
}

int ErasureCodeMlxJerasure::parse(ErasureCodeProfile &profile, ostream *ss)
{
  int err = ErasureCode::parse(profile, ss);

  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("m", profile, &m, DEFAULT_M, ss);
  err |= to_int("w", profile, &w, DEFAULT_W, ss);
  if (w != 1 && w != 2 && w != 4) {
    *ss << "ReedSolomonVandermonde: w=" << w
	<< " must be one of {1, 2, 4) revert to: " << DEFAULT_W << std::endl;
    err = -EINVAL;
    goto failed;
  }
  if (chunk_mapping.size() > 0 && (int)chunk_mapping.size() != k + m) {
    *ss << "mapping " << profile.find("mapping")->second
	<< " maps " << chunk_mapping.size() << " chunks instead of"
	<< " the expected " << k + m << " and will be ignored" << std::endl;
    chunk_mapping.clear();
    err = -EINVAL;
    goto failed;
  }

  err |= sanity_check_k(k, ss);

  err |= sanity_check_ec_capability(nic);
  dout(20) << __func__ << " check ec capability for nic with rc " << err << dendl;

failed:
  return err;
}

unsigned int ErasureCodeMlxJerasure::get_chunk_size(unsigned int obj_size) const
{
  unsigned align = get_alignment();
  unsigned int chunk_size = (obj_size + k - 1) / k;
  if (chunk_size % align) {
    unsigned pad = align - chunk_size % align;
    chunk_size += pad;
  }

  return chunk_size;
}

unsigned ErasureCodeMlxJerasure::get_alignment() const
{
  return MLX_ALIGNMENT_SIZE;
}

int ErasureCodeMlxJerasure::encode_chunks(const set<int>& want_to_read,
                                          map<int, bufferlist> *encoded)
{
  dout(20) << " encode want to read " << want_to_read << " block_size " \
           << (*encoded)[0].length() << dendl;

  char *chunks[k + m];
  for (int i = 0; i < k + m; i++)
    chunks[i] = (*encoded)[i].c_str();
  int r = jerasure_encode(&chunks[0], &chunks[k], (*encoded)[0].length());
  if (r)
    derr << " encode chunks err " << r << dendl;

  return r;
}

int ErasureCodeMlxJerasure::decode_chunks(const set<int>& want_to_read,
                                          const map<int, bufferlist>& chunks,
                                          map<int, bufferlist>* decoded)
{
  unsigned blocksize = (*chunks.begin()).second.length();
  int erasures[k + m + 1];
  int erasures_count = 0;
  char *data[k];
  char *coding[m];
  for (int i =  0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures[erasures_count] = i;
      erasures_count++;
    }
    if (i < k)
      data[i] = (*decoded)[i].c_str();
    else
      coding[i - k] = (*decoded)[i].c_str();
  }
  erasures[erasures_count] = -1;
  dout(20) << " decode want to read " << want_to_read << " with " \
           << erasures_count << " erased chunks " << erasures << dendl;

  assert(erasures_count > 0);
  int r = jerasure_decode(erasures, data, coding, blocksize);
  if(r)
    derr << " decode chunks err " << r <<  " with " << erasures_count \
         << " erased chunks " << erasures << dendl;

  return r;
}

int ErasureCodeMlxJerasure::jerasure_encode(char **data, char **coding,
                                            int blocksize)
{
  std::ostringstream ss;
  int r = encode_mlx_ec(ctx, data, coding, blocksize, &ss);
  if (r) 
    derr << " encode error " << ss.str() << " rc is " << r << dendl;
  return r;
}

int ErasureCodeMlxJerasure::jerasure_decode(int *erasure, char **data,
                                            char **coding, int blocksize)
{
  std::ostringstream ss; 
  int r = decode_mlx_ec(ctx, erasure, data, coding, blocksize, &ss);
  if (r)
    derr << " decode error " << ss.str() << " rc is " << r << dendl;
  return r;
}

