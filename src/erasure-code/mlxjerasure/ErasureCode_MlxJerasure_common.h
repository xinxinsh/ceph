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

#ifndef CEPH_ERASURE_CODE_MLX_JERASURE_COMMON_H
#define CEPH_ERASURE_CODE_MLX_JERASURE_COMMON_H

#include <iostream>
#include <sstream>
#include <infiniband/verbs_exp.h>

struct mlx_ec_memory_region {
  struct ibv_mr **mr;
  struct ibv_sge *sge;
};

struct mlx_ec_context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_exp_ec_calc *calc;
  struct ibv_exp_ec_calc_init_attr *attr;
  struct ibv_exp_ec_mem mem;
  int block_size;
  struct mlx_ec_memory_region *data;
  struct mlx_ec_memory_region *code;
  uint8_t *encode_matrix;
  uint8_t *decode_matrix;
  int *en_mtx;
  int *de_mtx;
  int *int_erasures;
  uint8_t *u8_erasures;
  int *survived_chunks;
};

int alloc_encode_matrix(struct mlx_ec_context* ctx, std::ostringstream *ss);
void free_encode_matrix(struct mlx_ec_context* ctx, std::ostringstream *ss);

int alloc_decode_matrix(struct mlx_ec_context* ctx, int *erasures, std::ostringstream *ss);
void free_decode_matrix(struct mlx_ec_contex* ctx, std::ostringstream *ss);

int alloc_mlx_ec_memory_region(struct mlx_ec_context* ctx, char **data,
                               char **coding, std::ostringstream *ss);
void free_mlx_ec_memory_region(struct mlx_ec_context* ctx, std::ostringstream *ss);

struct mlx_ec_context* alloc_mlx_ec_context(const char *dev,
                                            uint32_t max_inflight_calcs,
                                            int k, int m, int w,
                                            std::ostringstream *ss);
void free_mlx_ec_context(struct mlx_ec_context* ctx, std::ostringstream *ss);

int check_ec_capability(const char* name, std::ostringstream *ss);

int encode_mlx_ec(struct mlx_ec_context *ctx, char **data, char **coding,
                  int blocksize, std::ostringstream *ss);
int decode_mlx_ec(struct mlx_ec_context *ctx, int *erasures, char **data, 
                  char **coding, int blocksize, std::ostringstream *ss);

#endif
