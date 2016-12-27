// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include <errno.h>
#include <stdlib.h>

#include "crush/CrushWrapper.h"
#include "include/stringify.h"
#include "global/global_init.h"
#include "erasure-code/mlxjerasure/ErasureCodeMlxJerasure.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "common/config.h"
#include "gtest/gtest.h"

using std::cerr;

template <typename T>
class ErasureCodeTest : public ::testing::Test {
 public:
};

#define DEFAULT_MLX_NIC "mlx5_0"

TEST(ErasureCodeTest, init)
{
  ErasureCodeProfile profile;

  // check if w is in {1,2,4}
  {
    ErasureCodeMlxJerasure mlxjerasure(DEFAULT_MLX_NIC);
    profile["k"] = "2";
    profile["m"] = "2";
    profile["w"] = "8";

    EXPECT_EQ(-EINVAL, mlxjerasure.init(profile, &cerr));  
  }

  // check if the nic exists
  {
    ErasureCodeMlxJerasure mlxjerasure("mlx");
    profile["w"] = "4";

    EXPECT_EQ(-ENOENT, mlxjerasure.init(profile, &cerr));  
  }
  // check if the nic supports EC
  //profile["nic"] = NOTSUPPORT_NIC;
  //EXPECT_EQ(-ENOTSUP, mlxjerasure.init(profile, &cerr));  
  
}

TEST(ErasureCodeTest, encode_decode)
{
  ErasureCodeMlxJerasure mlxjerasure(DEFAULT_MLX_NIC);
  ErasureCodeProfile profile;
  
  int k=9, m=3;
  profile["k"] = "9";
  profile["m"] = "3";
  profile["w"] = "4";
  profile["nic"] = DEFAULT_MLX_NIC;

  mlxjerasure.init(profile, &cerr);

#define LARGE_ENOUGH 2048
  bufferptr in_ptr(buffer::create_page_aligned(LARGE_ENOUGH));
  in_ptr.zero();
  in_ptr.set_length(0);
  const char *payload =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  in_ptr.append(payload, strlen(payload));
  bufferlist in;
  in.push_front(in_ptr);
  int want_to_encode[k+m];
  for(int i=0; i<k+m; i++)
    want_to_encode[i] = i;
  map<int, bufferlist> encoded;
  EXPECT_EQ(0, mlxjerasure.encode(set<int>(want_to_encode, want_to_encode+k+m),
                                  in,
                                  &encoded));
  EXPECT_EQ(k+m, encoded.size());
  unsigned length = encoded[0].length();
  EXPECT_EQ(0, memcmp(encoded[0].c_str(), in.c_str(), length));

  // all chunks are available
  {
    int want_to_decode[] = { 0, 1 };
    map<int, bufferlist> decoded;
    EXPECT_EQ(0, mlxjerasure.decode(set<int>(want_to_decode, want_to_decode+2),
                                    encoded,
                                    &decoded));
    EXPECT_EQ(2u, decoded.size());
    EXPECT_EQ(length, decoded[0].length());
    EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
  }

  // two chunks are missing
  {
     map<int, bufferlist> degraded = encoded;
     degraded.erase(0);
     degraded.erase(1);
     EXPECT_EQ(k+m-2, degraded.size());
     int want_to_decode[] = { 0, 1 };
     map<int, bufferlist> decoded;
     EXPECT_EQ(0, mlxjerasure.decode(set<int>(want_to_decode, want_to_decode+2),
                                     degraded,
                                     &decoded));
     // always decode all, regardless of want_to_decode
     EXPECT_EQ(k+m, decoded.size());
     EXPECT_EQ(length, decoded[0].length());
     EXPECT_EQ(0, memcmp(decoded[0].c_str(), in.c_str(), length));
     EXPECT_EQ(0, memcmp(decoded[1].c_str(), in.c_str() + length, length));
  }
}

TEST(ErasureCodeTest, minimum_to_decode)
{
  ErasureCodeMlxJerasure mlxjerasure(DEFAULT_MLX_NIC);
  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  profile["w"] = "4";
  profile["nic"] = DEFAULT_MLX_NIC;
  mlxjerasure.init(profile, &cerr);

  // If trying to read nothing, the minimum is empty.
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    EXPECT_EQ(0, mlxjerasure.minimum_to_decode(want_to_read,
                                               available_chunks,
                                               &minimum));
    EXPECT_TRUE(minimum.empty());
  }

  // There is no way to read a chunk if none are available.
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);

    EXPECT_EQ(-EIO,  mlxjerasure.minimum_to_decode(want_to_read,
                                                   available_chunks,
                                                   &minimum));
  }

  // Reading a subset of the available chunks is always possible.
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    available_chunks.insert(0);

    EXPECT_EQ(0,  mlxjerasure.minimum_to_decode(want_to_read,
                                                available_chunks,
                                                &minimum));
    EXPECT_EQ(want_to_read, minimum);
  }

  // There is no way to read a missing chunk if there is less than k
  // chunks available.
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(0);
    want_to_read.insert(1);
    available_chunks.insert(0);

    EXPECT_EQ(-EIO,  mlxjerasure.minimum_to_decode(want_to_read,
                                                   available_chunks,
                                                   &minimum));
  }
  
  // When chunks are not available, the minimum can be made of any
  // chunks. For instance, to read 1 and 3 below the minimum could be
  // 2 and 3 which may seem better because it contains one of the
  // chunks to be read. But it won't be more efficient than retrieving
  // 0 and 2 instead because, in both cases, the decode function will
  // need to run the same recovery operation and use the same amount
  // of CPU and memory.
  {
    set<int> want_to_read;
    set<int> available_chunks;
    set<int> minimum;

    want_to_read.insert(1);
    want_to_read.insert(3);
    available_chunks.insert(0);
    available_chunks.insert(2);
    available_chunks.insert(3);

    EXPECT_EQ(0,  mlxjerasure.minimum_to_decode(want_to_read,
                                                available_chunks,
                                                &minimum));
    EXPECT_EQ(2u, minimum.size());
    EXPECT_EQ(0u, minimum.count(3));
  }
}

TEST(ErasureCodeTest, encode)
{
  ErasureCodeMlxJerasure mlxjerasure(DEFAULT_MLX_NIC);
  ErasureCodeProfile profile;
  profile["k"] = "2";
  profile["m"] = "2";
  profile["w"] = "4";
  mlxjerasure.init(profile, &cerr);

  unsigned aligned_object_size = mlxjerasure.get_alignment() * 2;
  {
    //
    // When the input bufferlist needs to be padded because
    // it is not properly aligned, it is padded with zeros.
    //
    bufferlist in;
    map<int,bufferlist> encoded;
    int want_to_encode[] = { 0, 1, 2, 3 };
    int trail_length = 1;
    in.append(string(aligned_object_size + trail_length, 'X'));
    EXPECT_EQ(0, mlxjerasure.encode(set<int>(want_to_encode, want_to_encode+4),
				 in,
				 &encoded));
    EXPECT_EQ(4u, encoded.size());
    char *last_chunk = encoded[1].c_str();
    int length =encoded[1].length();
    EXPECT_EQ('X', last_chunk[0]);
    EXPECT_EQ('\0', last_chunk[length - trail_length]);
  }

  {
    //
    // When only the first chunk is required, the encoded map only
    // contains the first chunk. Although the jerasure encode
    // internally allocated a buffer because of padding requirements
    // and also computes the coding chunks, they are released before
    // the return of the method, as shown when running the tests thru
    // valgrind (there is no leak).
    //
    bufferlist in;
    map<int,bufferlist> encoded;
    set<int> want_to_encode;
    want_to_encode.insert(0);
    int trail_length = 1;
    in.append(string(aligned_object_size + trail_length, 'X'));
    EXPECT_EQ(0, mlxjerasure.encode(want_to_encode, in, &encoded));
    EXPECT_EQ(1u, encoded.size());
  }
}

TEST(ErasureCodeTest, create_ruleset)
{
  CrushWrapper *c = new CrushWrapper;
  c->create();
  int root_type = 2;
  c->set_type_name(root_type, "root");
  int host_type = 1;
  c->set_type_name(host_type, "host");
  int osd_type = 0;
  c->set_type_name(osd_type, "osd");

  int rootno;
  c->add_bucket(0, CRUSH_BUCKET_STRAW, CRUSH_HASH_RJENKINS1,
		root_type, 0, NULL, NULL, &rootno);
  c->set_item_name(rootno, "default");

  map<string,string> loc;
  loc["root"] = "default";

  int num_host = 4;
  int num_osd = 5;
  int osd = 0;
  for (int h=0; h<num_host; ++h) {
    loc["host"] = string("host-") + stringify(h);
    for (int o=0; o<num_osd; ++o, ++osd) {
      c->insert_item(g_ceph_context, osd, 1.0, string("osd.") + stringify(osd), loc);
    }
  }

  {
    stringstream ss;
    ErasureCodeMlxJerasure mlxjerasure(DEFAULT_MLX_NIC);
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["w"] = "4";
    mlxjerasure.init(profile, &cerr);
    int ruleset = mlxjerasure.create_ruleset("myrule", *c, &ss);
    EXPECT_EQ(0, ruleset);
    EXPECT_EQ(-EEXIST, mlxjerasure.create_ruleset("myrule", *c, &ss));
    //
    // the minimum that is expected from the created ruleset is to
    // successfully map get_chunk_count() devices from the crushmap,
    // at least once.
    //
    vector<__u32> weight(c->get_max_devices(), 0x10000);
    vector<int> out;
    int x = 0;
    c->do_rule(ruleset, x, out, mlxjerasure.get_chunk_count(), weight);
    ASSERT_EQ(out.size(), mlxjerasure.get_chunk_count());
    for (unsigned i=0; i<out.size(); ++i)
      ASSERT_NE(CRUSH_ITEM_NONE, out[i]);
  }
  {
    stringstream ss;
    ErasureCodeMlxJerasure mlxjerasure(DEFAULT_MLX_NIC);
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["w"] = "4";
    profile["ruleset-root"] = "BAD";
    mlxjerasure.init(profile, &cerr);
    EXPECT_EQ(-ENOENT, mlxjerasure.create_ruleset("otherrule", *c, &ss));
    EXPECT_EQ("root item BAD does not exist", ss.str());
  }
  {
    stringstream ss;
    ErasureCodeMlxJerasure mlxjerasure(DEFAULT_MLX_NIC);
    ErasureCodeProfile profile;
    profile["k"] = "2";
    profile["m"] = "2";
    profile["w"] = "4";
    profile["ruleset-failure-domain"] = "WORSE";
    mlxjerasure.init(profile, &cerr);
    EXPECT_EQ(-EINVAL, mlxjerasure.create_ruleset("otherrule", *c, &ss));
    EXPECT_EQ("unknown type WORSE", ss.str());
  }
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  const char* env = getenv("CEPH_LIB");
  string directory(env ? env : "lib");
  g_conf->set_val("erasure_code_dir", directory, false, false);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/* 
 * Local Variables:
 * compile-command: "cd ../.. ;
 *   make -j4 unittest_erasure_code_jerasure &&
 *   valgrind --tool=memcheck \
 *      ./unittest_erasure_code_jerasure \
 *      --gtest_filter=*.* --log-to-stderr=true --debug-osd=20"
 * End:
 */
