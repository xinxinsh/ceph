// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "RocksDBStore.h"

#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include <errno.h>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"

using std::string;
#include "common/perf_counters.h"
#include "KeyValueDB.h"

int RocksDBStore::init(ostream &out, bool create_if_missing)
{
  rocksdb::Options ldoptions;

  if (options.write_buffer_size)
    ldoptions.write_buffer_size = options.write_buffer_size;
  if (options.max_open_files)
    ldoptions.max_open_files = options.max_open_files;
  if (options.cache_size) {
    ldoptions.block_cache = rocksdb::NewLRUCache(options.cache_size);
    //rocksdb::Cache *_db_cache = ldoptions.block_cache.get();
    //db_cache.reset(t);
  }
  if (options.block_size)
    ldoptions.block_size = options.block_size;
  if (options.bloom_size) {
    const rocksdb::FilterPolicy *_filterpolicy =
	rocksdb::NewBloomFilterPolicy(options.bloom_size);
    ldoptions.filter_policy = _filterpolicy;
    //filterpolicy.reset(&((void *)_filterpolicy));
  }
  if (options.compression_enabled)
    ldoptions.compression = rocksdb::kSnappyCompression;
  else
    ldoptions.compression = rocksdb::kNoCompression;
  if (options.block_restart_interval)
    ldoptions.block_restart_interval = options.block_restart_interval;

  ldoptions.error_if_exists = options.error_if_exists;
  ldoptions.paranoid_checks = options.paranoid_checks;
  ldoptions.create_if_missing = create_if_missing;

  if (options.log_file.length()) {
    rocksdb::Env *env = rocksdb::Env::Default();
    env->NewLogger(options.log_file, &ldoptions.info_log);
  }

  rocksdb::DB *_db;
  rocksdb::Status status = rocksdb::DB::Open(ldoptions, path, &_db);
  db.reset((void **)(&_db));
  if (!status.ok()) {
    out << status.ToString() << std::endl;
    return -EINVAL;
  }

  PerfCountersBuilder plb(g_ceph_context, "rocksdb", l_rocksdb_first, l_rocksdb_last);
  plb.add_u64_counter(l_rocksdb_gets, "rocksdb_get");
  plb.add_u64_counter(l_rocksdb_txns, "rocksdb_transaction");
  plb.add_u64_counter(l_rocksdb_compact, "rocksdb_compact");
  plb.add_u64_counter(l_rocksdb_compact_range, "rocksdb_compact_range");
  plb.add_u64_counter(l_rocksdb_compact_queue_merge, "rocksdb_compact_queue_merge");
  plb.add_u64(l_rocksdb_compact_queue_len, "rocksdb_compact_queue_len");
  logger = plb.create_perf_counters();
  cct->get_perfcounters_collection()->add(logger);
  return 0;
}

RocksDBStore::~RocksDBStore()
{
  close();
  delete logger;

  // Ensure db is destroyed before dependent db_cache and filterpolicy
  db.reset();
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
  rocksdb::DB *tdb = (rocksdb::DB *)(db.get());
  rocksdb::Status s = tdb->Write(rocksdb::WriteOptions(), (rocksdb::WriteBatch *)(_t->bat));
  logger->inc(l_rocksdb_txns);
  return s.ok() ? 0 : -1;
}

int RocksDBStore::submit_transaction_sync(KeyValueDB::Transaction t)
{
  RocksDBTransactionImpl * _t =
    static_cast<RocksDBTransactionImpl *>(t.get());
  rocksdb::WriteOptions options;
  options.sync = true;
  rocksdb::DB *tdb = (rocksdb::DB *)(db.get());
  rocksdb::Status s = tdb->Write(options, (rocksdb::WriteBatch *)(_t->bat));
  logger->inc(l_rocksdb_txns);
  return s.ok() ? 0 : -1;
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
  keys.push_back(key);
  rocksdb::WriteBatch *wbat = (rocksdb::WriteBatch *)bat;
  wbat->Delete(rocksdb::Slice(*(keys.rbegin())));
  wbat->Put(rocksdb::Slice(*(keys.rbegin())),
	  rocksdb::Slice(bl.c_str(), bl.length()));
}

void RocksDBStore::RocksDBTransactionImpl::rmkey(const string &prefix,
					         const string &k)
{
  string key = combine_strings(prefix, k);
  keys.push_back(key);
  rocksdb::WriteBatch *wbat = (rocksdb::WriteBatch *)bat;
  wbat->Delete(rocksdb::Slice(*(keys.rbegin())));
}

void RocksDBStore::RocksDBTransactionImpl::rmkeys_by_prefix(const string &prefix)
{
  KeyValueDB::Iterator it = db->get_iterator(prefix);
  for (it->seek_to_first();
       it->valid();
       it->next()) {
    string key = combine_strings(prefix, it->key());
    keys.push_back(key);
    rocksdb::WriteBatch *wbat = (rocksdb::WriteBatch *)bat;
    wbat->Delete(*(keys.rbegin()));
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

string RocksDBStore::combine_strings(const string &prefix, const string &value)
{
  string out = prefix;
  out.push_back(0);
  out.append(value);
  return out;
}

bufferlist RocksDBStore::to_bufferlist(void *in)
{
  bufferlist bl;
  rocksdb::Slice *sl = (rocksdb::Slice *)in;
  bl.append(bufferptr(sl->data(), sl->size()));
  return bl;
}

int RocksDBStore::split_key(void *in, string *prefix, string *key)
{
  rocksdb::Slice *sl = (rocksdb::Slice *)in;
  string in_prefix = sl->ToString();
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
  rocksdb::DB *tdb = (rocksdb::DB *)(*(db.get()));
  logger->inc(l_rocksdb_compact);
  tdb->CompactRange(NULL, NULL);
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
  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db;
  rocksdb::Status status = rocksdb::DB::Open(options, omap_dir, &db);
  return status.ok();
}
 void RocksDBStore::compact_range(const string& start, const string& end) {
    rocksdb::Slice cstart(start);
    rocksdb::Slice cend(end);
    rocksdb::DB *tdb = (rocksdb::DB *)(*(db.get()));
    tdb->CompactRange(&cstart, &cend);
  }

int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first() 
{
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  ldbiter->SeekToFirst();
  return ldbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_first(const string &prefix) 
{
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  rocksdb::Slice slice_prefix(prefix);
  ldbiter->Seek(slice_prefix);
  return ldbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last() 
{ 
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  ldbiter->SeekToLast();
  return ldbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::seek_to_last(const string &prefix) 
{
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  string limit = past_prefix(prefix);
  rocksdb::Slice slice_limit(limit);
  ldbiter->Seek(slice_limit);

  if (!ldbiter->Valid()) {
    ldbiter->SeekToLast();
  } else {
    ldbiter->Prev();
  }
  return ldbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::upper_bound(const string &prefix, const string &after) 
{
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  lower_bound(prefix, after);
  if (valid()) {
  pair<string,string> key = raw_key();
    if (key.first == prefix && key.second == after)
      next();
  }
  return ldbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::lower_bound(const string &prefix, const string &to) 
{
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  string bound = combine_strings(prefix, to);
  rocksdb::Slice slice_bound(bound);
  ldbiter->Seek(slice_bound);
  return ldbiter->status().ok() ? 0 : -1;
} 
bool RocksDBStore::RocksDBWholeSpaceIteratorImpl::valid()  
{ 
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  return ldbiter->Valid();
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::next() 
{ 
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  if (valid())
  ldbiter->Next();
  return ldbiter->status().ok() ? 0 : -1;
}
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::prev() 
{ 
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  if (valid())
    ldbiter->Prev();
    return ldbiter->status().ok() ? 0 : -1;
}
string RocksDBStore::RocksDBWholeSpaceIteratorImpl::key() 
{ 
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)(dbiter.get());
  string out_key;
  rocksdb::Slice sl = ldbiter->key();
  rocksdb::Slice *psl = &sl;
  split_key(psl, 0, &out_key);
  return out_key;
}
pair<string,string> RocksDBStore::RocksDBWholeSpaceIteratorImpl::raw_key() 
{
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)dbiter.get();
  string prefix, key;
  rocksdb::Slice sl = ldbiter->key();
  rocksdb::Slice *psl = &sl;
  split_key(psl, &prefix, &key);
  return make_pair(prefix, key);
} 
bufferlist RocksDBStore::RocksDBWholeSpaceIteratorImpl::value() 
{ 
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)(dbiter.get());
  rocksdb::Slice sl = ldbiter->value();
  rocksdb::Slice *psl = &sl;
  return to_bufferlist(psl);
} 
int RocksDBStore::RocksDBWholeSpaceIteratorImpl::status() 
{
  rocksdb::Iterator *ldbiter = (rocksdb::Iterator *)(dbiter.get());
  return ldbiter->status().ok() ? 0 : -1;
} 

bool RocksDBStore::in_prefix(const string &prefix, void *key) 
{
  rocksdb::Slice *k = (rocksdb::Slice *)key;
  return (k->compare(rocksdb::Slice(past_prefix(prefix))) < 0) &&
    (k->compare(rocksdb::Slice(prefix)) > 0);
}
string RocksDBStore::past_prefix(const string &prefix) 
{
  string limit = prefix;
  limit.push_back(1);
  return limit;
}


RocksDBStore::WholeSpaceIterator RocksDBStore::_get_iterator() 
{
  rocksdb::DB *tdb = (rocksdb::DB *)(*(db.get()));
  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBWholeSpaceIteratorImpl(
      tdb->NewIterator(rocksdb::ReadOptions())
    )
  );
}

RocksDBStore::WholeSpaceIterator RocksDBStore::_get_snapshot_iterator() 
{
  const rocksdb::Snapshot *snapshot;
  rocksdb::ReadOptions options;

  rocksdb::DB *tdb = (rocksdb::DB *)(*(db.get()));
  snapshot = tdb->GetSnapshot();
  options.snapshot = snapshot;

  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new RocksDBSnapshotIteratorImpl(tdb, snapshot,
      tdb->NewIterator(options))
  );
}

RocksDBStore::RocksDBSnapshotIteratorImpl::~RocksDBSnapshotIteratorImpl()
{
  rocksdb::Snapshot *snap = (rocksdb::Snapshot *)snapshot;
  rocksdb::DB *tdb = (rocksdb::DB *)db;
  tdb->ReleaseSnapshot(snap);
}
