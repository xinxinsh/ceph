
#ifndef CEPH_ROCKSDB_H
#define CEPH_ROCKSDB_H
#include "include/types.h"
#include "include/buffer.h"

#include <string>

using namespace std;

typedef void RocksDB_DB_HANDLE;
typedef void RocksDB_WRITEBATCH_HANDLE;
typedef void RocksDB_ITERATOR_HANDLE;
typedef void RocksDB_OPTIONS_HANDLE;
typedef void RocksDB_STATUS_HANDLE;
typedef void RocksDB_SNAPSHOT_HANDLE;
typedef void RocksDB_SLICE_HANDLE;
typedef void RocksDB_WRITEOPTIONS_HANDLE;
typedef void RocksDB_READOPTIONS_HANDLE;

class RocksDB_WriteBatch;
class RocksDB_Iterator;
class RocksDB_Options;
class RocksDB_Snapshot;
class RocksDB_Status;
class RocksDB_Slice;
class RocksDB_WriteOptions;
class RocksDB_ReadOptions;

class RocksDB_DB
{
  public:
    RocksDB_DB_HANDLE *db;
    RocksDB_DB();
    RocksDB_DB(void *_db);
    ~RocksDB_DB();
    //RocksDB_DB_HANDLE * operator->();
    RocksDB_DB_HANDLE * get();
    bool write(RocksDB_WriteBatch &bat);
    bool write_sync(RocksDB_WriteBatch &bat);
    void compact_range(const string &start, const string &end);
    RocksDB_Iterator * getNewIterator(RocksDB_ReadOptions &option);
    RocksDB_Snapshot * getSnapshot();
    void releaseSnapshot(RocksDB_Snapshot &snapshot);
}; 
class RocksDB_WriteBatch
{ 
  public:
    RocksDB_WRITEBATCH_HANDLE *wbat;
    RocksDB_WriteBatch();
    RocksDB_WriteBatch(void *_wbat);
    ~RocksDB_WriteBatch();
    //RocksDB_WRITEBATCH_HANDLE & operator*();
    RocksDB_WRITEBATCH_HANDLE * operator->();
    RocksDB_WRITEBATCH_HANDLE * get();
    void rmkey(string &key);
    void setkey(string &key, bufferlist &bl);
}; 
class RocksDB_Iterator
{ 
  public:
    RocksDB_ITERATOR_HANDLE *iter;
    RocksDB_Iterator();
    RocksDB_Iterator(void *_iter);
    ~RocksDB_Iterator();
    //RocksDB_ITERATOR_HANDLE & operator*();
    RocksDB_ITERATOR_HANDLE * operator->();
    RocksDB_ITERATOR_HANDLE * get();
    int seek_to_first();
    int seek_to_first(const string &prefix);
    int seek_to_last();
    int seek_to_last(const string &prefix);
    int upper_bound(const string &prefix, const string &after);
    int lower_bound(const string &prefix, const string &to);
    bool valid();
    int prev();
    int next();
    string key();
    pair<string,string>raw_key();
    bufferlist value();
    int status();
    static int split_key(string str, string *prefix, string *key);
}; 
class RocksDB_Options
{  
  public:
    RocksDB_OPTIONS_HANDLE *options;
    RocksDB_Options();
    RocksDB_Options(void *_options);
    ~RocksDB_Options();
    //RocksDB_OPTIONS_HANDLE & operator*();
    RocksDB_OPTIONS_HANDLE * operator->();
    RocksDB_OPTIONS_HANDLE * get();
    void set_write_buffer_size(uint64_t size);
    void set_max_open_files(int max_open_files);
    void set_cache_size(uint64_t cache_size);
    void set_block_size(uint64_t block_size);
    void set_bloom_size(int bloom_size);
    void set_compression_enabled(bool compression_enabled);
    void set_block_restart_interval(int block_restart_interval);
    void set_error_if_exists(bool error_if_exist);
    void set_paranoid_checks(bool paranoid_checks);
    void set_create_if_missing(bool create_if_missing);
    void set_log_file(string log_file);
};  
class RocksDB_Status
{
  public:
    RocksDB_STATUS_HANDLE *status;
    RocksDB_Status();
    RocksDB_Status(void *_status);
    ~RocksDB_Status();
    //RocksDB_STATUS_HANDLE & operator*();
    RocksDB_STATUS_HANDLE * operator->();
    RocksDB_STATUS_HANDLE * get();
    bool ok();
};    
class RocksDB_Snapshot
{  
  public:
    const RocksDB_SNAPSHOT_HANDLE *snapshot;
    RocksDB_Snapshot();
    RocksDB_Snapshot(const void *_snapshot);
 //   ~RocksDB_Snapshot();
    //RocksDB_SNAPSHOT_HANDLE & operator*();
    const RocksDB_SNAPSHOT_HANDLE * operator->();
    const RocksDB_SNAPSHOT_HANDLE * get();
};  
class RocksDB_Slice
{  
  public:
    RocksDB_SLICE_HANDLE *slice;
    RocksDB_Slice();
    RocksDB_Slice(string &_slice);
    RocksDB_Slice(const char *data, int size);
    RocksDB_Slice(void *_slice);
    ~RocksDB_Slice();
    //RocksDB_SLICE_HANDLE & operator*();
    RocksDB_SLICE_HANDLE * operator->();
    RocksDB_SLICE_HANDLE * get();
    const char * data();
    size_t size();
    string ToString();   
};  
class RocksDB_WriteOptions
{  
  public:
    RocksDB_WRITEOPTIONS_HANDLE *writeoptions;
    RocksDB_WriteOptions();
    RocksDB_WriteOptions(void *_writeoptions);
    ~RocksDB_WriteOptions();
    //RocksDB_SLICE_HANDLE & operator*();
    RocksDB_WRITEOPTIONS_HANDLE * operator->();
    RocksDB_WRITEOPTIONS_HANDLE * get();
};  
class RocksDB_ReadOptions
{  
  public:
    RocksDB_READOPTIONS_HANDLE *readoptions;
    RocksDB_ReadOptions();
    RocksDB_ReadOptions(void *_readoptions);
    ~RocksDB_ReadOptions();
    //RocksDB_SLICE_HANDLE & operator*();
    RocksDB_READOPTIONS_HANDLE * operator->();
    RocksDB_READOPTIONS_HANDLE * get();
    void set_snapshot(RocksDB_Snapshot &snapshot);
};   

bool RocksDB_Init(const string &dir,RocksDB_Options &options, RocksDB_DB &db);
#endif
