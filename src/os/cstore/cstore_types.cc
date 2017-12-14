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

ostream& operator<<(ostream& os, objnode& o) {
  os << "block size " << o.block_size << "  ";
  os << "compression type " << o.get_alg_str() << "\n";
  o.blocks.hexdump(os);
  return os;
}

void intrusive_ptr_add_ref(objnode* o) {
  o->get();
}

void intrusive_ptr_release(objnode* o) {
  o->put();
}

objnode::objnode(const ghobject_t &o, uint64_t block_size, uint64_t size) 
: o(o), ref(0), block_size(block_size), size(size), c_type(0) {
  uint64_t bits = P2ROUNDUP(size, block_size) / block_size;
  uint64_t len = P2ROUNDUP(bits, 8) / 8;
  blocks.append_zero(len);
}

void objnode::set_size(uint64_t nsize) {
  uint64_t bits = P2ROUNDUP(nsize, block_size) / block_size;
  uint64_t len = P2ROUNDUP(bits, 8) / 8;
  if ((nsize > size) && (len > blocks.length())) {
    blocks.append_zero(len - blocks.length());
  } else if ((nsize < size) && (len < blocks.length())) {
    bufferlist bp;
    bp.substr_of(blocks, 0, len);
    blocks.clear();
    blocks.append(bp);
  }
  size = nsize;
}

void objnode::update_blocks(uint64_t off, uint64_t len) {
  if (off + len > size)
    set_size(off+len);
  uint64_t s = P2ALIGHN(off, block_size) / block_size;
  uint64_t e = P2ROUNDUP(off+len, block_size) / block_size;
  char * p = blocks.c_str();
  for(; s<e; s++) {
    uint64_t which_byte = s / 8;
    uint64_t which_bit = s % 8;
    p[which_byte] |= (1 << which_bit);
  }
}

int objnode::get_next_set_block(uint64_t start, uint64_t* next) {
  char* p = blocks.c_str();
  uint64_t bits = blocks.length() << 3;
  while(start < bits) {
    if (p[(start / 8)] & (1 << (start % 8))) {
      *next = start;
      return 0;
    }
    ++start;
  }
  return -1;
}

objnode::state_t objnode::get_alg_type(const string &type) {
  if (type == "none") return COMP_ALG_NONE;
  else if (type == "snappy") return COMP_ALG_SNAPPY;
  else return COMP_ALG_UNKNOW;
}

const char* objnode::get_alg_str() {
  switch(c_type) {
    case COMP_ALG_NONE: return "none";
    case COMP_ALG_SNAPPY: return "snappy";
    default: return "???";
  }
}

bool objnode::is_compressed() {
  switch (c_type) {
    case COMP_ALG_NONE: return false;
    case COMP_ALG_SNAPPY: return true;
    default: return false;
  }
}

void objnode::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(size, bl);
  ::encode(block_size, bl);
  ::encode(c_type, bl);
  ::encode(blocks, bl);
  ENCODE_FINISH(bl);
}

void objnode::decode(bufferlist::iterator &bl) {
  DECODE_START(1, bl);
  ::decode(size, bl);
  ::decode(block_size, bl);
  ::decode(c_type, bl);
  ::decode(blocks, bl);
  DECODE_FINISH(bl);
}

ostream& operator<<(ostream& os, compression_header& c) {
  os << "oid " << c.oid << " state " << c.get_state_str();
  return os;
}

void compression_header::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(cid, bl);
  ::encode(oid, bl);
  ::encode(state, bl);
  ENCODE_FINISH(bl);
}

void compression_header::decode(bufferlist::iterator &bl) {
  DECODE_START(1, bl);
  ::decode(cid, bl);
  ::decode(oid, bl);
  ::decode(state, bl);
  DECODE_FINISH(bl);
}

const char* compression_header::get_state_str() {
  switch(state) {
    case STATE_INIT: 
      return "INIT";
    case STATE_PROGRESS:
      return "PROGRESS";
    default:
      return "???";
  }
}

