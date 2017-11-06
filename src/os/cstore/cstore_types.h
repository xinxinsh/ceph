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

class objnode {
public:
  objnode() : c(coll_t()), o(ghobject_t()), block_size(0), size(0), c_type(0) {}
  objnode(const coll_t &c, const ghobject_t &o, uint64_t block_size, uint64_t size);
  virtual ~objnode(){}
  void set_size(uint64_t nsize);
  uint32_t get_size() {return size;}
  void set_alg_type(uint8_t ntype);
  uint8_t get_alg_type() {return c_type;}
  string get_alg_str();
  void update_blocks(uint64_t off, uint64_t len);
  int get_next_set_block(int start);
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

  std::atomic_int ref;
  coll_t c;
  ghobject_t o;
  uint64_t block_size;
  uint64_t size;
  uint8_t c_type;
  bufferlist blocks;
};
typedef boost::intrusive_ptr<objnode> ObjnodeRef;
WRITE_CLASS_ENCODER(objnode)

#endif
