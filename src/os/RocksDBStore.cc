// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "RocksDBStore.h"
#include "RocksDB.h"

#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include <errno.h>

/*
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
*/
using std::string;
#include "common/perf_counters.h"
#include "KeyValueDB.h"

class RocksDB_WriteBatch;
class RocksDB_Iterator;
class RocksDB_Options;
class RocksDB_Snapshot;
class RocksDB_Status;
class RocksDB_Slice;
class RocksDB_WriteOptions;
class RocksDB_ReadOptions;


int RocksDBStore::init(ostream &out, bool create_if_missing)
{
  RocksDB_Options ldoptions;

  if (options.write_buffer_size)
    ldoptions.set_write_buffer_size(options.write_buffer_size);
  if (options.max_open_files)
    ldoptions.set_max_open_files(options.max_open_files);
  if (options.cache_size) {
    ldoptions.set_cache_size(options.cache_size);
  }
  if (options.block_size)
    ldoptions.set_block_size(options.block_size);
  if (options.bloom_size) 
    ldoptions.set_bloom_size(options.bloom_size);
  ldoptions.set_compression_enabled(options.compression_enabled);
  if (options.block_restart_interval)
    ldoptions.set_block_restart_interval(options.block_restart_interval);

  ldoptions.set_error_if_exists(options.error_if_exists);
  ldoptions.set_paranoid_checks(options.paranoid_checks);
  ldoptions.set_create_if_missing(create_if_missing);
  if(options.log_file.length())
    ldoptions.set_log_file(options.log_file);

  RocksDB_DB _db;
  if (!RocksDB_Init(path,ldoptions,_db)) {
    std::cout << "init rocksdb error" << std::endl;
    return -EINVAL;
  }
  db = &_db;

  std::cout << " build perfcounter for rocksdb " << std::endl;
  PerfCountersBuilder plb(g_ceph_context, "rocksdb", l_rocksdb_first, l_rocksdb_last);
  plb.add_u64_counter(l_rocksdb_gets, "rocksdb_get");
  plb.add_u64_counter(l_rocksdb_txns, "rocksdb_transaction");
  plb.add_u64_counter(l_rocksdb_compact, "rocksdb_compact");
  plb.add_u64_counter(l_rocksdb_compact_range, "rocksdb_compact_range");
  plb.add_u64_counter(l_rocksdb_compact_queue_merge, "rocksdb_compact_queue_merge");
  plb.add_u64(l_rocksdb_compact_queue_len, "rocksdb_compact_queue_len");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  std::cout << "add perfcounter to ceph context " << std::endl;
  return 0;
}

RocksDBStore::~RocksDBStore()
{
  close();
  delete logger;

  // Ensure db is destroyed before dependent db_cache and filterpolicy
  std::cout << "start release db " << std::endl;
  //db.reset();
  delete db;
  std::cout << "finish release db " << std::endl;
}

void RocksDBStore::close()
{
  // stop compaction thread
  compact_queue_lock.Lock();
  if (compact_thread.is_started()) {
    compact_queue_stop = true;
    compact_queue_cond.Signal();
    compact_queue_lock.Unlock();
    compact_thread.join();
  } else {
    compact_queue_lock.Unlock();
  }

  if (logger)
    cct->get_perfcounters_collection()->remove(logger);
}

int RocksDBStore::submit_transaction(KeyValueDB::Transaction t)
{
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
//  RocksDB_DB *tdb = (RocksDB_DB *)(db.get());
  bool ok = db->write(_t->bat);
  logger->inc(l_rocksdb_txns);
  return ok ? 0 : -1;
}

int RocksDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  //RocksDB_DB *tdb = (RocksDB_DB *)(db.get());
  bool ok = db->write(_t->bat);
  logger->inc(l_rocksdb_txns);
  return ok ? 0 : -1;
}

void RocksDBStore::RocksDBTransactionImpl::set(
  const string &prefix,
  const string &k,
  const bufferlist &to_set_bl)
{
  buffers.push_back(to_set_bl);
  buffers.rbegin()->rebuild();
  bufferlist &bl = *(buffers.rbegin());
  string key = combine_strings(prefix, k);
 // RocksDB_WriteBatch *wbat = (RocksDB_WriteBatch *)bat;
  bat.rmkey(*(keys.rbegin()));
  bat.setkey(*(keys.rbegin()),bl);
}

void RocksDBStore::RocksDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  //RocksDB_WriteBatch *wbat = (RocksDB_WriteBatch *)bat;
  bat.rmkey(*(keys.rbegin()));
}

void RocksDBStore::RocksDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    keys.push_back(key);
    bat.rmkey(*(keys.rbegin()));
  }
}

int RocksDBStore::get(
    const string &prefix,
    const std::set<string> &keys,
    std::map<string, bufferlist> *out)
{
  KeyValueDB::Iterator it = get_iterator(prefix);
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    it->lower_bound(*i);
    if (it->valid() && it->key() == *i) {
      out->insert(make_pair(*i, it->value()));
    } else if (!it->valid())
      break;
  }
  logger->inc(l_rocksdb_gets);
  return 0;
}

bufferlist RocksDBStore::to_bufferlist(RocksDB_Slice *in)
{
  bufferlist bl;
//  RocksDB_Slice *sl = (RocksDB_Slice *)in;
  bl.append(bufferptr(in->data(), in->size()));
  return bl;
}

int RocksDBStore::split_key(RocksDB_Slice *in, string *prefix, string *key)
{
//  RocksDB_Slice *sl = (RocksDB_Slice *)in;
  string in_prefix = in->ToString();
  size_t prefix_len = in_prefix.find('\0');
  if (prefix_len >= in_prefix.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(in_prefix, 0, prefix_len);
  if (key)
    *key= string(in_prefix, prefix_len + 1);
  return 0;
}

void RocksDBStore::compact()
{
  logger->inc(l_rocksdb_compact);
  db->compact_range(NULL, NULL);
}


void RocksDBStore::compact_thread_entry()
{
  compact_queue_lock.Lock();
  while (!compact_queue_stop) {
    while (!compact_queue.empty()) {
      pair<string,string> range = compact_queue.front();
      compact_queue.pop_front();
      logger->set(l_rocksdb_compact_queue_len, compact_queue.size());
      compact_queue_lock.Unlock();
      logger->inc(l_rocksdb_compact_range);
      compact_range(range.first, range.second);
      compact_queue_lock.Lock();
      continue;
    }
    compact_queue_cond.Wait(compact_queue_lock);
  }
  compact_queue_lock.Unlock();
}

void RocksDBStore::compact_range_async(const string& start, const string& end)
{
  Mutex::Locker l(compact_queue_lock);

  // try to merge adjacent ranges.  this is O(n), but the queue should
  // be short.  note that we do not cover all overlap cases and merge
  // opportunities here, but we capture the ones we currently need.
  list< pair<string,string> >::iterator p = compact_queue.begin();
  while (p != compact_queue.end()) {
    if (p->first == start && p->second == end) {
      // dup; no-op
      return;
    }
    if (p->first <= end && p->first > start) {
      // merge with existing range to the right
      compact_queue.push_back(make_pair(start, p->second));
      compact_queue.erase(p);
      logger->inc(l_rocksdb_compact_queue_merge);
      break;
    }
    if (p->second >= start && p->second < end) {
      // merge with existing range to the left
      compact_queue.push_back(make_pair(p->first, end));
      compact_queue.erase(p);
      logger->inc(l_rocksdb_compact_queue_merge);
      break;
    }
    ++p;
  }
  if (p == compact_queue.end()) {
    // no merge, new entry.
    compact_queue.push_back(make_pair(start, end));
    logger->set(l_rocksdb_compact_queue_len, compact_queue.size());
  }
  compact_queue_cond.Signal();
  if (!compact_thread.is_started()) {
    compact_thread.create();
  }
}
bool RocksDBStore::check_omap_dir(string &omap_dir)
{
  RocksDB_Options options;
  bool create_if_missing = true;
  options.set_create_if_missing(create_if_missing);
  RocksDB_DB db;
  bool check = RocksDB_Init(omap_dir, options, db);
  return check;
}
void RocksDBStore::compact_range(const string& start, const string& end) 
{
    db->compact_range(start, end);
}

int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first() 
{
  return dbiter->seek_to_first();
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first(const string &prefix) 
{
  return dbiter->seek_to_first(prefix);
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last() 
{ 
  return dbiter->seek_to_last();
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last(const string &prefix) 
{
  return dbiter->seek_to_last(prefix);
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after) 
{
  return dbiter->upper_bound(prefix,after);
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to) 
{
  return dbiter->lower_bound(prefix,to);
} 
bool RocksDBStore::RocksDBWholeSpaceIteratorImpl::valid()  
{ 
  return dbiter->valid();
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::next() 
{ 
  return dbiter->next();
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::prev() 
{ 
  return dbiter->prev();
}
string RocksDBStore::RocksDBWholeSpaceIteratorImpl::key() 
{ 
  return dbiter->key();
}
pair<string,string> RocksDBStore::RocksDBWholeSpaceIteratorImpl::raw_key() 
{
  return dbiter->raw_key();
} 
bufferlist RocksDBStore::RocksDBWholeSpaceIteratorImpl::value() 
{ 
  return dbiter->value();
} 
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::status() 
{
  return dbiter->status();
} 

string RocksDBStore::past_prefix(const string &prefix) 
{
  string limit = prefix;
  limit.push_back(1);
  return limit;
}

string RocksDBStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}


RocksDBStore::WholeSpaceIterator RocksDBStore::_get_iterator() 
{
  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBWholeSpaceIteratorImpl(
      db->getNewIterator(*(new RocksDB_ReadOptions()))
    )
  );
}

RocksDBStore::WholeSpaceIterator RocksDBStore::_get_snapshot_iterator() 
{

  RocksDB_Snapshot *snapshot = db->getSnapshot();
  RocksDB_ReadOptions *readoption = new RocksDB_ReadOptions();
  readoption->set_snapshot(*snapshot);

  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBSnapshotIteratorImpl(db, snapshot,
      db->getNewIterator(*readoption))
  );
}

RocksDBStore::RocksDBSnapshotIteratorImpl::~RocksDBSnapshotIteratorImpl()
{
  db->releaseSnapshot(*snapshot);
}
