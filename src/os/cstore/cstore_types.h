/*
 * =====================================================================================
 *
 *       Filename:  cstore_type.h
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
  objnode() : c(coll_t()), o(ghobject_t()), block_size(0), size(0), type(0) {}
  objnode(coll_t &c, ghobject_t &o, uint32_t block_size, uint32_t size);
  virtual ~objnode(){}
  void set_size(uint32_t nsize);
  uint32_t get_size() {return size;}
  void set_alg_type(uint8_t ntype);
  uint8_t get_alg_type() {return type;}
  void update_blocks(uint32_t off, uint32_t len);
  int get_next_set_block(uint32_t off, uint32_t *biti);
  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &bl);
  friend ostream& operator<<(ostream& os, objnode& o);

  coll_t c;
  ghobject_t o;
  uint32_t block_size;
  uint32_t size;
  uint8_t type;
  bufferlist blocks;
};
WRITE_CLASS_ENCODER(objnode)

#endif
