/*
 * =====================================================================================
 *
 *       Filename:  cstore_types.h
 *
 *    Description:  metadata types of Chinac Store
 *
 *        Version:  1.0
 *        Created:  10/31/2017 04:01:44 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  shu xinxin (shuxinxin@huayun.com), 
 *   Organization:  huayun
 *
 * =====================================================================================
 */

#ifndef CEPH_OS_CSTORE_TYPES_H
#define CEPH_OS_CSTORE_TYPES_H

#include <include/buffer.h>
#include <osd/osd_types.h>
#include <include/types.h>
#include <include/encoding.h>

#define ISP2(x) (((x) & ((x) - 1)) == 0)
#define P2ALIGHN(x, y) ((x) & -(y))
#define P2ROUNDUP(x, y) (-(-(x) & -(y)))

class objnode {
public:
  enum state_t {
    COMP_ALG_UNKNOW = 0,
    COMP_ALG_NONE,
    COMP_ALG_SNAPPY,
  };

  objnode() : o(ghobject_t()), ref(0), block_size(0), size(0), c_type(COMP_ALG_NONE) {}
  objnode(const ghobject_t &o, uint64_t block_size, uint64_t size);
  virtual ~objnode(){}
  void set_size(uint64_t nsize);
  uint32_t get_size() {return size;}
  objnode::state_t get_alg_type(const string &type);
  const char* get_alg_str();
  void update_blocks(uint64_t off, uint64_t len);
  int get_next_set_block(uint64_t start, uint64_t *next);
  bool is_compressed();
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  friend ostream& operator<<(ostream& os, objnode& o);
  friend void intrusive_ptr_add_ref(objnode* o);
  friend void intrusive_ptr_release(objnode* o);
  void get() {
    ++ref;
  }
  void put() {
    if(--ref == 0)
      delete this;
  }

  ghobject_t o;
  std::atomic_int ref;
  uint64_t block_size;
  uint64_t size;
  uint8_t c_type;
  bufferlist blocks;
};
typedef boost::intrusive_ptr<objnode> ObjnodeRef;
WRITE_CLASS_ENCODER(objnode)

class compression_header {
public:
  enum state_t {
    STATE_NONE = 0,
    STATE_INIT,
    STATE_PROGRESS,
  };

  compression_header(const coll_t& cid, const ghobject_t& oid, state_t state) : cid(cid), oid(oid), state(state) {}
  friend ostream& operator<<(ostream& os, compression_header& c);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  const char* get_state_str();

  coll_t cid;
  ghobject_t oid;
  uint8_t state;
};
WRITE_CLASS_ENCODER(compression_header)

#endif
