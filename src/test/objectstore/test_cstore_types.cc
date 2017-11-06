/*
 * =====================================================================================
 *
 *       Filename:  test_cstore_type.cc
 *
 *    Description:  Test of CStore Type
 *
 *        Version:  1.0
 *        Created:  11/01/2017 02:23:02 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  shu xinxin (shuxinxin@huayun.com), 
 *   Organization:  
 *
 * =====================================================================================
 */

#include "gtest/gtest.h"
#include "os/cstore/cstore_types.h"
#include "include/encoding.h"

class cstore_types_objnode_test : public ::testing::Test {
public:
  objnode *on;
  void SetUp() {
    coll_t *c = new coll_t();
    ghobject_t *o = new ghobject_t();
    on = new objnode(*c, *o, 4*1024, 8);
  }
  void TearDown() {
    delete on;
  }
};

TEST_F(cstore_types_objnode_test, objnode_update) {
  EXPECT_EQ(1, on->blocks.length());
  uint32_t size = 4 * 1024 * 1024;
  on->set_size(size);
  EXPECT_EQ(size, on->get_size());
  EXPECT_EQ(128, on->blocks.length());
}

TEST_F(cstore_types_objnode_test, objnode_set_alg_type) {
  EXPECT_FALSE(on->is_compressed());
  on->c_type = objnode::COMP_ALG_SNAPPY;
  EXPECT_TRUE(on->is_compressed());
}

TEST_F(cstore_types_objnode_test, objnode_get_alg_type) {
  on->c_type = objnode::COMP_ALG_NONE;
  EXPECT_STREQ(on->get_alg_str(), "none");
  on->c_type = objnode::COMP_ALG_SNAPPY;
  EXPECT_STREQ(on->get_alg_str(), "snappy");
}

TEST_F(cstore_types_objnode_test, objnode_blocks) {
  int r;
  uint64_t n;
  uint32_t size = 4 * 1024 * 1024;
  on->set_size(size);
  on->update_blocks(52210, 40977);
  r = on->get_next_set_block(0, &n);
  EXPECT_EQ(0, r);
  EXPECT_EQ(12, n);
  r = on->get_next_set_block(22, &n);
  EXPECT_EQ(0, r);
  EXPECT_EQ(22, n);
  r = on->get_next_set_block(24, &n);
  EXPECT_EQ(-1, r);
  on->update_blocks(52210, 42977);
  r = on->get_next_set_block(0, &n);
  EXPECT_EQ(12, n);
  r = on->get_next_set_block(23, &n);
  EXPECT_EQ(23, n);
  r = on->get_next_set_block(25, &n);
  EXPECT_EQ(-1, r);
}

TEST_F(cstore_types_objnode_test, encode_decode) {
  bufferlist bl;
  bufferlist::iterator p;
  objnode node;
  ::encode(*on, bl);
  p = bl.begin();
  ::decode(node, p);
  EXPECT_EQ(on->size, node.size);
  EXPECT_EQ(on->block_size, node.block_size);
  EXPECT_EQ(on->c_type, node.c_type);
  EXPECT_STREQ(on->blocks.c_str(), node.blocks.c_str());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
