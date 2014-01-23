
#include "rocksdb/db.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/status.h"

/*
typedef rocksdb::DB RocksDB_DB_HANDLE;
typedef rocksdb::WriteBatch RocksDB_WRITEBATCH_HANDLE;
typedef rocksdb::Iterator RocksDB_ITERATOR_HANDLE;
typedef rocksdb::Options RocksDB_OPTIONS_HANDLE;
typedef rocksdb::Status RocksDB_STATUS_HANDLE;
typedef rocksdb::Snapshot RocksDB_SNAPSHOT_HANDLE;
typedef rocksdb::Slice RocksDB_SLICE_HANDLE;
typedef rocksdb::WriteOptions RocksDB_WRITEOPTIONS_HANDLE;
typedef rocksdb::ReadOptions RocksDB_READOPTIONS_HANDLE;
*/

#include "RocksDB.h"

RocksDB_DB::RocksDB_DB()
{
  rocksdb::DB *_db ;
  db = _db;
}
RocksDB_DB::RocksDB_DB(void *_db)
{
  db = _db;
}
RocksDB_DB::~RocksDB_DB()
{
  rocksdb::DB *_db = (rocksdb::DB *)db;
  delete _db;
  db = NULL;
}
/*
RocksDB_DB_HANDLE & RocksDB_DB::operator*()
{
  return *db;
}
RocksDB_DB_HANDLE * RocksDB_DB::operator->()
{
  return db;
}
*/
RocksDB_DB_HANDLE * RocksDB_DB::get()
{
  return db;
}
void RocksDB_DB::compact_range(const string &start, const string &end)
{
  rocksdb::DB *tdb = (rocksdb::DB *)db;
  const rocksdb::Slice *s = new rocksdb::Slice(start);
  const rocksdb::Slice *e = new rocksdb::Slice(end);
  tdb->CompactRange(s,e);
}
bool RocksDB_DB::write(RocksDB_WriteBatch &bat)
{
  rocksdb::DB *tdb = (rocksdb::DB *)db;
  rocksdb::WriteBatch *tbat = (rocksdb::WriteBatch *)bat.wbat;
  rocksdb::Status status = tdb->Write(rocksdb::WriteOptions(),tbat);
  return status.ok();
}
bool RocksDB_DB::write_sync(RocksDB_WriteBatch &bat)
{
  rocksdb::DB *tdb = (rocksdb::DB *)db;
  rocksdb::WriteBatch *tbat = (rocksdb::WriteBatch *)bat.wbat;
  rocksdb::WriteOptions wopt;
  wopt.sync=true;
  rocksdb::Status status = tdb->Write(wopt,tbat);
  return status.ok();
}
RocksDB_Iterator * RocksDB_DB::getNewIterator(RocksDB_ReadOptions &readoption)
{
  rocksdb::ReadOptions *readopt = (rocksdb::ReadOptions *)readoption.get();
  rocksdb::DB *tdb = (rocksdb::DB *)db;
  return new RocksDB_Iterator(tdb->NewIterator(*readopt));
}
RocksDB_Snapshot * RocksDB_DB::getSnapshot()
{
  rocksdb::DB *tdb = (rocksdb::DB *)db;
  return new RocksDB_Snapshot(tdb->GetSnapshot());
}
void RocksDB_DB::releaseSnapshot(RocksDB_Snapshot &snapshot)
{
  rocksdb::DB *tdb = (rocksdb::DB *)db;
  rocksdb::Snapshot *snap = (rocksdb::Snapshot *)(snapshot.get());
  tdb->ReleaseSnapshot(snap);
}
RocksDB_Iterator::RocksDB_Iterator()
{
  rocksdb::Iterator *_iter;
  iter = _iter;
}
RocksDB_Iterator::RocksDB_Iterator(void *_iter)
{
  iter = _iter;
}
RocksDB_Iterator::~RocksDB_Iterator()
{
  rocksdb::Iterator *_iter = (rocksdb::Iterator *)iter;
  delete _iter;
  iter = NULL;
}
RocksDB_ITERATOR_HANDLE * RocksDB_Iterator::operator ->()
{
  return iter;
}
RocksDB_ITERATOR_HANDLE * RocksDB_Iterator::get()
{
  return iter;
}
int RocksDB_Iterator::seek_to_first() 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  dbiter->SeekToFirst();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDB_Iterator::seek_to_first(const string &prefix) 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  rocksdb::Slice slice_prefix(prefix);
  dbiter->Seek(slice_prefix);
  return dbiter->status().ok() ? 0 : -1;
}     
int RocksDB_Iterator::seek_to_last() 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  dbiter->SeekToLast();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDB_Iterator::seek_to_last(const string &prefix) 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  string limit = prefix;
  limit.push_back(1);
  rocksdb::Slice slice_limit(limit);
  dbiter->Seek(slice_limit);

   if (!dbiter->Valid()) 
   { 
     dbiter->SeekToLast();
   } else {
     dbiter->Prev();
   }
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDB_Iterator::upper_bound(const string &prefix, const string &after) 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  lower_bound(prefix, after);
  if (valid()) 
  {
    pair<string,string> key = raw_key();
    if (key.first == prefix && key.second == after)
    next();
  }
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDB_Iterator::lower_bound(const string &prefix, const string &to) 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  string bound = prefix;
  bound.push_back(0);
  dbiter->Seek(bound);
  return dbiter->status().ok() ? 0 : -1;
}
bool RocksDB_Iterator::valid() 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  return dbiter->Valid();
}
int RocksDB_Iterator::next() 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  if (valid())
    dbiter->Next();
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDB_Iterator::prev() 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  if (valid())
    dbiter->Prev();
  return dbiter->status().ok() ? 0 : -1;
}
string RocksDB_Iterator::key() 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  string out_key;
  string prefix(0);
  split_key(dbiter->key().ToString(), &prefix, &out_key);
  return out_key;
}
pair<string,string> RocksDB_Iterator::raw_key() 
{ 
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  string prefix, key;
  split_key(dbiter->key().ToString(), &prefix, &key);
  return make_pair(prefix, key);
}
bufferlist RocksDB_Iterator::value() 
{
  bufferlist bl;
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  rocksdb::Slice sl = dbiter->value();
  bl.append(bufferptr(sl.data(),sl.size()));
  return bl;
}
int RocksDB_Iterator::status() 
{
  rocksdb::Iterator *dbiter = (rocksdb::Iterator *)iter;
  return dbiter->status().ok() ? 0 : -1;
}
int RocksDB_Iterator::split_key(string str, string *prefix, string *key)
{
  size_t prefix_len = str.find('\0');
  if (prefix_len >= str.size())
    return -EINVAL;

  if (prefix)
    *prefix = string(str, 0, prefix_len);
  if (key)
    *key= string(str, prefix_len + 1);
  return 0;
}
RocksDB_WriteBatch::RocksDB_WriteBatch()
{
  rocksdb::WriteBatch *_wbat = new rocksdb::WriteBatch();
  wbat = _wbat;
}
RocksDB_WriteBatch::RocksDB_WriteBatch(void *_wbat)
{
  wbat = (rocksdb::WriteBatch *)_wbat;
}
RocksDB_WriteBatch::~RocksDB_WriteBatch()
{
  rocksdb::WriteBatch *_wbat = (rocksdb::WriteBatch *)wbat;
  delete _wbat;
  wbat = NULL;
}
/*
rocksdb::WriteBatch & RocksDB_WriteBatch::operator*()
{
  return *wbat;
}
*/
RocksDB_WRITEBATCH_HANDLE * RocksDB_WriteBatch::operator->()
{
  return wbat;
}
RocksDB_WRITEBATCH_HANDLE * RocksDB_WriteBatch::get()
{
  return wbat;
}
void RocksDB_WriteBatch::rmkey(string &key)
{
  rocksdb::WriteBatch *tbat = (rocksdb::WriteBatch *)wbat;
  tbat->Delete(rocksdb::Slice(key));
}
void RocksDB_WriteBatch::setkey(string &key, bufferlist &bl)
{
  rocksdb::WriteBatch *tbat = (rocksdb::WriteBatch *)wbat;
  tbat->Delete(rocksdb::Slice(key));
  tbat->Put(rocksdb::Slice(key),rocksdb::Slice(bl.c_str(),bl.length()));
}
RocksDB_Options::RocksDB_Options()
{
  rocksdb::Options *_options = new rocksdb::Options();
  options = _options;
}
RocksDB_Options::RocksDB_Options(void *_options)
{
  options = (rocksdb::Options *)_options;
}
RocksDB_Options::~RocksDB_Options()
{
  rocksdb::Options *_options = (rocksdb::Options *)options;
  delete _options;
  options = NULL;
}
/*
rocksdb::Options & RocksDB_Options::operator*()
{
  return *options;
}
*/
RocksDB_OPTIONS_HANDLE * RocksDB_Options::operator->()
{
  return options;
}
RocksDB_OPTIONS_HANDLE * RocksDB_Options::get()
{
  return options;
}
void RocksDB_Options::set_write_buffer_size(uint64_t size)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->write_buffer_size = size;
}
void RocksDB_Options::set_max_open_files(int max_open_files)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->max_open_files = max_open_files;
}
void RocksDB_Options::set_cache_size(uint64_t cache_size)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->block_cache = rocksdb::NewLRUCache(cache_size);
}
void RocksDB_Options::set_block_size(uint64_t block_size)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->block_size = block_size;
}
void RocksDB_Options::set_bloom_size(int bloom_size)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->filter_policy = rocksdb::NewBloomFilterPolicy(bloom_size);
}
void RocksDB_Options::set_block_restart_interval(int block_restart_interval)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->block_restart_interval = block_restart_interval;
}
void RocksDB_Options::set_error_if_exists(bool error_if_exists)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->error_if_exists = error_if_exists;
}
void RocksDB_Options::set_paranoid_checks(bool paranoid_checks)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->paranoid_checks = paranoid_checks;
}
void RocksDB_Options::set_compression_enabled(bool compression_enabled)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  if (compression_enabled)
    opt->compression = rocksdb::kSnappyCompression;
  else
    opt->compression = rocksdb::kNoCompression;
}
void RocksDB_Options::set_create_if_missing(bool create_if_missing)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  opt->create_if_missing = create_if_missing;
}
void RocksDB_Options::set_log_file(string log_file)
{
  rocksdb::Options *opt = (rocksdb::Options *)options;
  rocksdb::Env *env = rocksdb::Env::Default();
  env->NewLogger(log_file, &opt->info_log);
}
RocksDB_Status::RocksDB_Status()
{
  rocksdb::Status *_status = new rocksdb::Status();
  status = _status;
}
RocksDB_Status::RocksDB_Status(void *_status)
{
  status = (rocksdb::Status *)_status;
}
RocksDB_Status::~RocksDB_Status()
{
  rocksdb::Status *_status = (rocksdb::Status *)status;
  delete _status;
  status = NULL;
}
/*
rocksdb::Status & RocksDB_Status::operator*()
{
  return *status;
}
*/
RocksDB_STATUS_HANDLE * RocksDB_Status::operator->()
{
  return status;
}
RocksDB_STATUS_HANDLE * RocksDB_Status::get()
{
  return status;
}
RocksDB_Snapshot::RocksDB_Snapshot()
{
  rocksdb::Snapshot *_snapshot = new rocksdb::Snapshot;
  snapshot = _snapshot;
}
RocksDB_Snapshot::RocksDB_Snapshot(const void *_snapshot)
{
  snapshot = _snapshot;
}
/*
RocksDB_Snapshot::~RocksDB_Snapshot()
{
  //delete snapshot;
}
rocksdb::Snapshot & RocksDB_Snapshot::operator*()
{
  return *snapshot;
}
*/
const RocksDB_SNAPSHOT_HANDLE * RocksDB_Snapshot::operator->()
{
  return snapshot;
}
const RocksDB_SNAPSHOT_HANDLE * RocksDB_Snapshot::get()
{
  return snapshot;
}
RocksDB_Slice::RocksDB_Slice()
{
  rocksdb::Slice *_slice = new rocksdb::Slice();
  slice = _slice;
}
RocksDB_Slice::RocksDB_Slice(string &_slice)
{
  rocksdb::Slice sl = rocksdb::Slice(_slice);
  slice = &sl;
}
RocksDB_Slice::RocksDB_Slice(const char *data, int size)
{
  rocksdb::Slice sl = rocksdb::Slice(data,size);
  slice = &sl;
}
RocksDB_Slice::RocksDB_Slice(void *_slice)
{
  slice = _slice;
}
RocksDB_Slice::~RocksDB_Slice()
{
  rocksdb::Slice *_slice = (rocksdb::Slice *)slice;
  delete _slice;
  slice = NULL;
}
/*
RocksDB_SLICE_HANDLE & RocksDB_Slice::operator*()
{
  return *slice;
}
*/
RocksDB_SLICE_HANDLE * RocksDB_Slice::operator->()
{
  return slice;
}
RocksDB_SLICE_HANDLE * RocksDB_Slice::get()
{
  return slice;
}
const char * RocksDB_Slice::data()
{
  rocksdb::Slice *sl = (rocksdb::Slice *)slice;
  return sl->data();
}
size_t RocksDB_Slice::size()
{
  rocksdb::Slice *sl = (rocksdb::Slice *)slice;
  return sl->size();
}
string RocksDB_Slice::ToString()
{
  rocksdb::Slice *sl = (rocksdb::Slice *)slice;
  return sl->ToString();
}
RocksDB_WriteOptions::RocksDB_WriteOptions()
{
  rocksdb::WriteOptions *_writeoptions = new rocksdb::WriteOptions();
  writeoptions = _writeoptions;
}
RocksDB_WriteOptions::RocksDB_WriteOptions(void *_writeoptions)
{
  writeoptions = (rocksdb::WriteOptions *)_writeoptions;
}
RocksDB_WriteOptions::~RocksDB_WriteOptions()
{
  rocksdb::WriteOptions *_writeoptions = (rocksdb::WriteOptions *)writeoptions;
  delete _writeoptions;
  writeoptions = NULL;
}
/*
rocksdb::WriteOptions & RocksDB_WriteOptions::operator*()
{
  return *writeoptions;
}
*/
RocksDB_WRITEOPTIONS_HANDLE * RocksDB_WriteOptions::operator->()
{ 
  return writeoptions;
}
RocksDB_WRITEOPTIONS_HANDLE * RocksDB_WriteOptions::get()
{
  return writeoptions;
}
RocksDB_ReadOptions::RocksDB_ReadOptions()
{
  rocksdb::ReadOptions *_readoptions = new rocksdb::ReadOptions();
  readoptions = _readoptions;
}
RocksDB_ReadOptions::RocksDB_ReadOptions(void *_readoptions)
{
  readoptions = (rocksdb::ReadOptions *)_readoptions;
}
RocksDB_ReadOptions::~RocksDB_ReadOptions()
{
  rocksdb::ReadOptions *_readoptions = (rocksdb::ReadOptions *)readoptions;
  delete _readoptions;
  readoptions = NULL;
}
/*
rocksdb::ReadOptions & RocksDB_ReadOptions::operator*()
{
  return *readoptions;
}
*/
RocksDB_READOPTIONS_HANDLE * RocksDB_ReadOptions::operator->()
{
  return readoptions;
}
RocksDB_READOPTIONS_HANDLE * RocksDB_ReadOptions::get()
{
  return readoptions;
}
void RocksDB_ReadOptions::set_snapshot(RocksDB_Snapshot &snapshot)
{
  rocksdb::ReadOptions *readopt = (rocksdb::ReadOptions *)readoptions;
  readopt->snapshot = (const rocksdb::Snapshot *)snapshot.get();
}
bool RocksDB_Init(const string &dir, RocksDB_Options &options, RocksDB_DB &db)
{
  rocksdb::Options *_opt = (rocksdb::Options *)options.options;
  rocksdb::DB *_db = (rocksdb::DB *)db.db;
  rocksdb::Status status = rocksdb::DB::Open(*_opt, dir, &_db);
  return status.ok();
}
