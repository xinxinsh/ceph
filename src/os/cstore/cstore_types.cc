/*
 * =====================================================================================
 *
 *       Filename:  cstore_type.cc
 *
 *    Description:  Implementation metadata types
 *
 *        Version:  1.0
 *        Created:  10/31/2017 04:38:37 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  shu xinxin (shuxinxin@huayun.com)
 *   Organization:  huayun
 *
 * =====================================================================================
 */

#include "cstore_types.h"

#define ISP2(x) (((x) & ((x) - 1)) == 0)
#define P2ALIGHN(x, y) ((x) & -(y))
#define P2ROUNDUP(x, y) (-(-(x) & -(y)))

ostream& operator<<(ostream& os, objnode& o) {
  os << "block size " << o.block_size << "  ";
  os << "compression type " << o.c_type << "  ";
  os << o.blocks << "\n";
  return os;
}

objnode::objnode(coll_t &c, ghobject_t &o, uint32_t block_size, uint32_t size) 
: c(c), o(o), block_size(block_size), size(size), c_type(0) {
  uint32_t bits = P2ROUNDUP(size, block_size) / block_size;
  uint32_t len = P2ROUNDUP(bits, 8) / 8;
  blocks.append_zero(len);
}

void objnode::set_size(uint32_t nsize) {
  if (nsize > size) {
    size = nsize;
    uint32_t bits = P2ROUNDUP(size, block_size) / block_size;
    uint32_t len = P2ROUNDUP(bits, 8) / 8;
    if (len > blocks.length())
      blocks.append_zero(len - blocks.length());
  }
}

void objnode::update_blocks(uint32_t off, uint32_t len) {
  if (off + len > size)
    set_size(off+len);
  uint32_t s = P2ALIGHN(off, block_size) / block_size;
  uint32_t e = P2ROUNDUP(off+len, block_size) / block_size;
  char * p = blocks.c_str();
  for(; s<e; s++) {
    uint32_t which_byte = s / 8;
    uint32_t which_bit = s % 8;
    p[which_byte] |= (1 << which_bit);
  }
}

int objnode::get_next_set_block(uint32_t off, uint32_t* biti) {
  uint32_t next_off = P2ROUNDUP(off, block_size);
  if ((off > size) || (next_off >= size)) return -1;
  uint32_t s = next_off / block_size;
  uint32_t e = P2ROUNDUP(size, block_size) / block_size;
  char* p = blocks.c_str();
  for(; s<e; s++) {
    if (p[(s / 8)] & (1 << (s % 8))) {
      *biti = s;
      return 0;
    }
  }
  return -1;
}

void objnode::set_alg_type(uint8_t ntype) {
  c_type = ntype;
}

bool objnode::is_compressed() {
  if (c_type > 0)
    return true;
  return false;
}

void objnode::encode(bufferlist &bl) const {
  ENCODE_START(2, 2, bl);
  ::encode(size, bl);
  ::encode(block_size, bl);
  ::encode(c_type, bl);
  ::encode(blocks, bl);
  ENCODE_FINISH(bl);
}

void objnode::decode(bufferlist::iterator &bl) {
  DECODE_START(2, bl);
  ::decode(size, bl);
  ::decode(block_size, bl);
  ::decode(c_type, bl);
  ::decode(blocks, bl);
  DECODE_FINISH(bl);
}

