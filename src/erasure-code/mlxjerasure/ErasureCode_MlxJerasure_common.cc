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

#include <infiniband/verbs_exp.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


#include "ErasureCode_MlxJerasure_common.h"
extern "C" {
#include "jerasure.h"
#include "reed_sol.h"
}

int alloc_encode_matrix(struct mlx_ec_context* ctx, std::ostringstream *ss)
{
  int i, j, k, m, w, r = 0;
  int *matrix;
  
  k = ctx->attr->k;
  m = ctx->attr->m;
  w = ctx->attr->w;

  ctx->encode_matrix = (uint8_t*)calloc(k*m, 1);
  if (!ctx->encode_matrix) {
    *ss << " failed to alloc_encode_matrix " << std::endl;
    r = -ENOMEM;
    goto failed;
  }

  matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
  if (!matrix) {
    *ss << " failed to alloc_encode_matrix " << std::endl;
    r = -EINVAL;
    goto free_matrix;
  }
  
  for(i=0; i < m; i++)
    for(j=0; j < k; j++)
      ctx->encode_matrix[j*m+i] = (uint8_t)(matrix[i*k+j]);
  
  
  ctx->en_mtx = matrix;

  return r;

free_matrix:
  free(matrix);
failed:
  return r;
}

void free_encode_matrix(struct mlx_ec_context* ctx, std::ostringstream *ss)
{
  free(ctx->en_mtx);
  free(ctx->encode_matrix);
}

int alloc_decode_matrix(struct mlx_ec_context* ctx, int *erasures, std::ostringstream *ss)
{
  int *matrix;
  uint8_t *dematrix;
  int r = 0, l = 0;
  int k = ctx->attr->k;
  int m = ctx->attr->m;
  int w = ctx->attr->w;
  int erased = 0;

  ctx->int_erasures = (int *)calloc(k+m, sizeof(int));
  if (!ctx->int_erasures) {
    *ss << " failed to alloc int_erasures " << std::endl;
    r = -ENOMEM;
    goto failed;
  }

  ctx->u8_erasures = (uint8_t *)calloc(k+m, sizeof(uint8_t));
  if (!ctx->u8_erasures) {
    *ss << " failed to alloc u8_erasures " << std::endl;
    r = -ENOMEM;
    goto free_int_erasures;
  }

  for (int i = 0; erasures[i] != -1; i++) {
    if (ctx->int_erasures[erasures[i]] == 0) {
      ctx->int_erasures[erasures[i]] = 1;
      ctx->u8_erasures[erasures[i]] = 1;
      erased++;
    }
    if (erased > m) {
      *ss << " missing too many chunks " << std::endl;
      r = -EINVAL;
      goto free_u8_erasures;
    }
  }

  matrix = (int *)calloc(k*k, sizeof(int));
  if (!matrix) {
    *ss << " failed to alloc_decode_matrix " << std::endl;
    r = -ENOMEM;
    goto free_int_erasures;
  }

  // decode matrix is array with k*erased length, where the k is 
  // # of data chunks and erased is the # of erased chunks, for 
  // example, for k=5,m=3 algorithm, two chunks are erased, so
  // erased=2, so length of decode matrix is 10.
  
  dematrix = (uint8_t *)calloc(erased * k, 1);
  if (!dematrix) {
    *ss << " failed to alloc decode matrix" << std::endl;
    r = -ENOMEM;
    goto free_matrix;
  }

  ctx->survived_chunks = (int *)calloc(k, sizeof(int));
  if (!ctx->survived_chunks) {
    *ss << " failed to alloc buf for survived chunks " << std::endl;
    r = -ENOMEM;
    goto free_dematrix;
  }

  r = jerasure_make_decoding_matrix(k, erased, w, ctx->en_mtx,
                                    ctx->int_erasures, matrix, 
                                    ctx->survived_chunks);
  if (r) {
    *ss << " failed to alloc decoding matrix " << std::endl;
    r = -EINVAL;
    goto free_survived_chunks;
  } 

  for (int i = 0; i < k + erased; i++) {
    if (ctx->int_erasures[i]) {
      for (int j = 0; j < k; j++) {
        dematrix[j*erased + l] = (uint8_t)(matrix[i*k + j]);
      }
      l++;
    }
  }
  
  ctx->de_mtx = matrix;
  ctx->decode_matrix = dematrix;
  
  return r;

free_survived_chunks:
  free(ctx->survived_chunks);
free_dematrix:
  free(dematrix);
free_matrix:
  free(matrix); 
free_u8_erasures:
  free(ctx->u8_erasures);
free_int_erasures:
  free(ctx->int_erasures);
failed:
  return r;
}

void free_decode_matrix(struct mlx_ec_context* ctx, std::ostringstream *ss)
{
  free(ctx->survived_chunks);
  free(ctx->int_erasures);
  free(ctx->decode_matrix);
}

int alloc_mlx_ec_memory_region(struct mlx_ec_context* ctx, char **data,
                               char **coding, std::ostringstream *ss)
{
  int r = 0;

  ctx->data = (struct mlx_ec_memory_region *)calloc(1, sizeof(struct mlx_ec_memory_region));
  if (!ctx->data) {
    *ss << " cannot alloc data memory region " << std::endl;
    r = -ENOMEM;
    goto failed;
  }
  
  ctx->code = (struct mlx_ec_memory_region *)calloc(1, sizeof(struct mlx_ec_memory_region));
  if (!ctx->code) {
    *ss << " cannot alloc code memory region " << std::endl;
    r = -ENOMEM;
    goto free_data;
  }

  ctx->data->mr = (struct ibv_mr**)calloc(ctx->attr->k, sizeof(struct ibv_mr*));
  if (!ctx->data->mr) {
    *ss << " cannot register data buf to protection domain " << std::endl;
    r = -EINVAL;
    goto free_code;
  }

  for(int i=0; i < ctx->attr->k; i++) {
    (ctx->data->mr)[i] = ibv_reg_mr(ctx->pd, data[i], ctx->block_size,
                                    IBV_ACCESS_LOCAL_WRITE);
  }

  ctx->code->mr = (struct ibv_mr**)calloc(ctx->attr->m, sizeof(struct ibv_mr*));
  if (!ctx->code->mr) {
    *ss << " cannot register data buf to protection domain " << std::endl;
    r = -EINVAL;
    goto free_data_mr;
  }

  for(int i=0; i < ctx->attr->m; i++) {
    (ctx->code->mr)[i] = ibv_reg_mr(ctx->pd, coding[i], ctx->block_size,
                                    IBV_ACCESS_LOCAL_WRITE);
  }

  if (!ctx->code->mr) {
    *ss << " cannot register code buf to protection domain " << std::endl;
    r = -EINVAL;
    goto free_data_mr;
  }

  ctx->data->sge = (struct ibv_sge*)calloc(1, ctx->attr->k * sizeof(struct ibv_sge));
  if (!ctx->data->sge) {
    *ss << " cannot alloc data sge " << std::endl;
    r = -ENOMEM;
    goto free_code_mr;
  }

  for(int i = 0; i < ctx->attr->k; i++) {
    ctx->data->sge[i].lkey = (ctx->data->mr)[i]->lkey;
    ctx->data->sge[i].addr = (uintptr_t)data[i];
    ctx->data->sge[i].length = ctx->block_size;
  }

  ctx->code->sge = (struct ibv_sge*)calloc(1, ctx->attr->m * sizeof(struct ibv_sge));
  if (!ctx->code->sge) {
    *ss << " cannot alloc code sge " << std::endl;
    r = -ENOMEM;
    goto free_data_sge;
  }

  for(int i = 0; i < ctx->attr->m; i++) {
    ctx->code->sge[i].lkey = (ctx->code->mr)[i]->lkey;
    ctx->code->sge[i].addr = (uintptr_t)coding[i];
    ctx->code->sge[i].length = ctx->block_size;
  }

  ctx->mem.data_blocks = ctx->data->sge;
  ctx->mem.num_data_sge = ctx->attr->k;
  ctx->mem.code_blocks = ctx->code->sge;
  ctx->mem.num_code_sge = ctx->attr->m;
  ctx->mem.block_size = ctx->block_size;

  return 0;  

free_data_sge:
  free(ctx->data->sge);
free_code_mr:
  for(int i=0; i < ctx->attr->m; i++)
    ibv_dereg_mr((ctx->code->mr)[i]);
free_data_mr:
  for(int i=0; i < ctx->attr->k; i++)
    ibv_dereg_mr((ctx->data->mr)[i]);
free_code:
  free(ctx->code);
free_data:
  free(ctx->data);
failed: 
  return r;
}

void free_mlx_ec_memory_region(struct mlx_ec_context* ctx, std::ostringstream *ss)
{
  free(ctx->code->sge);
  free(ctx->data->sge);
  for(int i=0; i<ctx->attr->k; i++)
    ibv_dereg_mr((ctx->code->mr)[i]);
  for(int i=0; i<ctx->attr->m; i++)
    ibv_dereg_mr((ctx->data->mr)[i]);
  free(ctx->code);
  free(ctx->data);
}

struct mlx_ec_context* alloc_mlx_ec_context(const char *dev,
                                            uint32_t max_inflight_calcs,
                                            int k, int m, int w,
                                            std::ostringstream *ss)
{
  struct mlx_ec_context* ctx;
  int i, r = 0;
  struct ibv_device **device_list = NULL;
  struct ibv_device *device = NULL;

  device_list = ibv_get_device_list(NULL);
  if(!device_list) {
    *ss << " NO IB devices found " << std::endl;
    return NULL;
  }

  for(i = 0; device_list[i]; i++) {
    if (!strcmp(ibv_get_device_name(device_list[i]), dev))
      break;
  }

  device = device_list[i];
  if (!device) {
    *ss << "No IB device found with name " << dev << std::endl;
    return NULL;
  }
  
  ctx = (struct mlx_ec_context *)calloc(1, sizeof(struct mlx_ec_context));
  if (!ctx) {
    *ss << " cannot alloc mlx_ec_context " << std::endl;
    return NULL;
  }

  ctx->ctx = ibv_open_device(device);
  if (!ctx->ctx) {
    *ss << " failed to open device " << std::endl;
    goto free_ec_ctx;
  }

  ctx->pd = ibv_alloc_pd(ctx->ctx);
  if (!ctx->pd) {
    *ss << " failed alloc pd " << std::endl;
    goto free_ibv_ctx;
  }

  ctx->attr = (struct ibv_exp_ec_calc_init_attr *)calloc(1, sizeof(struct ibv_exp_ec_calc_init_attr));
  if (!ctx->attr) {
    *ss << " cannot alloc ibv_exp_ec_calc_init_attr " << std::endl;
    goto free_ibv_pd;
  }

  ctx->attr->max_inflight_calcs = max_inflight_calcs;
  ctx->attr->k = k;
  ctx->attr->m = m;
  ctx->attr->w = w;
  ctx->attr->max_data_sge = k;
  ctx->attr->max_code_sge = m;
  ctx->attr->affinity_hint = 0;
  ctx->attr->comp_mask = IBV_EXP_EC_CALC_ATTR_MAX_INFLIGHT | 
                         IBV_EXP_EC_CALC_ATTR_K | 
                         IBV_EXP_EC_CALC_ATTR_M |
                         IBV_EXP_EC_CALC_ATTR_W |
                         IBV_EXP_EC_CALC_ATTR_MAX_DATA_SGE |
                         IBV_EXP_EC_CALC_ATTR_MAX_CODE_SGE |
                         IBV_EXP_EC_CALC_ATTR_ENCODE_MAT |
                         IBV_EXP_EC_CALC_ATTR_AFFINITY |
                         IBV_EXP_EC_CALC_ATTR_POLLING;

  r = alloc_encode_matrix(ctx, ss);
  if (r) {
    *ss << " cannot alloc_encode_matrix " << std::endl;
    goto free_attr;
  }

  ctx->attr->encode_matrix = ctx->encode_matrix;

  ctx->calc = ibv_exp_alloc_ec_calc(ctx->pd, ctx->attr);
  if (!ctx->calc) {
    *ss << " cannot ibv_exp_alloc_ec_calc " << std::endl;
    goto free_encode;
  }

  return ctx;

free_encode:
  free_encode_matrix(ctx, ss);
free_attr:
  free(ctx->attr);
free_ibv_pd:
  free(ctx->pd);
free_ibv_ctx:
  free(ctx->ctx);
free_ec_ctx:
  free(ctx);

  return NULL;
}

void free_mlx_ec_context(struct mlx_ec_context* ctx, std::ostringstream *ss)
{
  ibv_exp_dealloc_ec_calc(ctx->calc);
  free(ctx->encode_matrix);
  free_mlx_ec_memory_region(ctx, ss);
  free(ctx->attr);
  free(ctx);
}

int check_ec_capability(const char* name, std::ostringstream *ss)
{
  struct ibv_context *ctx;
  struct ibv_exp_device_attr attr;
  int i, r = 0;
  struct ibv_device **device_list = NULL;
  struct ibv_device *device = NULL;

  if (!name) {
    *ss << " do not specify IB device " << std::endl;
    return -ENOMEM;
  }

  device_list = ibv_get_device_list(NULL);
  if (!device_list) {
    *ss << " No IB device found " << std::endl;
    return -ENOENT;
  }

  for(i = 0; device_list[i]; i++) {
    if (!strcmp(ibv_get_device_name(device_list[i]), name))
      break;
  }

  if (!device_list[i]) {
    *ss << " No IB device found with name " << name << std::endl;
    return -ENOENT;
  }

  device = device_list[i];

  ctx = ibv_open_device(device);
  if (!ctx) {
    *ss << " cannot alloc context of device " << ibv_get_device_name(device) << std::endl;
    return -EBADF;
  } 

  memset(&attr, 0, sizeof(attr));
  attr.comp_mask = IBV_EXP_DEVICE_ATTR_EXP_CAP_FLAGS | IBV_EXP_DEVICE_ATTR_EC_CAPS;
  r = ibv_exp_query_device(ctx, &attr);
  if (r) {
    *ss << " cannot query device for EC offload capability " << std::endl;
    r = -EACCES;
    goto close_device;
  }

  if (!(attr.exp_device_cap_flags & IBV_EXP_DEVICE_EC_OFFLOAD)) {
    *ss << " EC offlaod not supported by the driver " << std::endl;
    r = -ENOTSUP;
    goto close_device;
  }

  ibv_free_device_list(device_list);

close_device:
  ibv_close_device(ctx);

  return r;
}

int encode_mlx_ec(struct mlx_ec_context *ctx, char **data, char **coding,
                  int blocksize, std::ostringstream *ss)
{
  int r = 0;

  ctx->block_size = blocksize;
  ctx->mem.block_size = blocksize;
  
  r = alloc_mlx_ec_memory_region(ctx, data, coding, ss);
  if (r) {
    *ss << " cannot alloc_ec_memory_region " << std::endl;
    r = -ENOMEM;
    goto failed;
  }
  
  r = alloc_encode_matrix(ctx, ss);
  if (r) {
    *ss << " failed to alloc encode matrix " << std::endl;
    goto free_memory;
  }

  r = ibv_exp_ec_encode_sync(ctx->calc, &ctx->mem);
  if (r) {
    *ss << " failed to sync encode " << std::endl;
    r = -EINVAL;
    goto free_encode_matrix;
  }

  return r;

free_encode_matrix:
  free_encode_matrix(ctx, ss);
free_memory:
  free_mlx_ec_memory_region(ctx, ss);
failed:
  return r;
}

int decode_mlx_ec(struct mlx_ec_context *ctx, int *erasures, char **data,
                  char **coding, int blocksize, std::ostringstream *ss)
{
  int r = 0;
  ctx->block_size = blocksize;
  ctx->mem.block_size = blocksize;
  
  r = alloc_mlx_ec_memory_region(ctx, data, coding, ss);
  if (r) {
    *ss << " cannot alloc_ec_memory_region " << std::endl;
    r = -ENOMEM;
    goto failed;
  }

  r = alloc_decode_matrix(ctx, erasures, ss);
  if (r) {
    *ss << " failed to allocate decode matrix " << std::endl;
    goto free_memory;
  }
  
  r = ibv_exp_ec_decode_sync(ctx->calc, &ctx->mem, ctx->u8_erasures, ctx->decode_matrix);
  if (r) {
    *ss << " failed to sync decode " << std::endl;
    r = -EINVAL;
    goto free_decode_matrix;
  }

  return r;

free_decode_matrix:
  free_decode_matrix(ctx, ss);
free_memory:
  free_mlx_ec_memory_region(ctx, ss);
failed:
  return r;
}
