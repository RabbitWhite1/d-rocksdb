//
//  rocksdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_ROCKSDB_DB_H_
#define YCSB_C_ROCKSDB_DB_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "core/properties.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace ycsbc {

class RocksdbDB : public DB {
 public:
  RocksdbDB() {}
  ~RocksdbDB() {
    if (db_) {
      const std::lock_guard<std::mutex> lock(mu_);
      delete db_;
      db_ = nullptr;
    }
  }

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(table, key, fields, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return (this->*(method_scan_))(table, key, len, fields, result);
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_update_))(table, key, values);
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(table, key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(table, key);
  }

  void GetOrPrintDBStatus(std::map<std::string, std::string>* status_map, bool should_print, bool reset_stats) {
    std::map<std::string, std::string> cache_status;
    assert(db_ != nullptr);
    db_->GetMapProperty(rocksdb::DB::Properties::kBlockCacheEntryStats, &cache_status);

    std::shared_ptr<rocksdb::Statistics> statistics = db_->GetOptions().statistics;
    uint64_t block_cache_hit, block_cache_miss;
    uint64_t block_cache_data_hit, block_cache_data_miss;
    uint64_t block_cache_compression_dict_hit, block_cache_compression_dict_miss;
    uint64_t memtable_hit, memtable_miss;
    double block_cache_hit_ratio, block_cache_data_hit_ratio, block_cache_compression_dict_hit_ratio, memtable_hit_ratio;
    if (statistics != nullptr) {
      block_cache_hit = statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
      block_cache_miss = statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
      block_cache_hit_ratio = (double)block_cache_hit / (block_cache_hit + block_cache_miss) * 100;
      block_cache_data_hit = statistics->getTickerCount(rocksdb::BLOCK_CACHE_DATA_HIT);
      block_cache_data_miss = statistics->getTickerCount(rocksdb::BLOCK_CACHE_DATA_MISS);
      block_cache_data_hit_ratio = (double)block_cache_data_hit / (block_cache_data_hit + block_cache_data_miss) * 100;
      block_cache_compression_dict_hit = statistics->getTickerCount(rocksdb::BLOCK_CACHE_COMPRESSION_DICT_HIT);
      block_cache_compression_dict_miss = statistics->getTickerCount(rocksdb::BLOCK_CACHE_COMPRESSION_DICT_MISS);
      block_cache_compression_dict_hit_ratio = (double)block_cache_compression_dict_hit / (block_cache_compression_dict_hit + block_cache_compression_dict_miss) * 100;
      memtable_hit = statistics->getTickerCount(rocksdb::MEMTABLE_HIT);
      memtable_miss = statistics->getTickerCount(rocksdb::MEMTABLE_MISS);
      memtable_hit_ratio = (double)memtable_hit / (memtable_hit + memtable_miss) * 100;
    }

    // try to print stats
    if (should_print) {
      printf("Block cache entry stats(count,bytes,percent): "
             "DataBlock(%s, %s, %s), IndexBlock(%s, %s, %s), FilterBlock(%s, %s, %s), Misc(%s, %s, %s)\n",
             cache_status["count.data-block"].c_str(), cache_status["bytes.data-block"].c_str(), cache_status["percent.data-block"].c_str(),
             cache_status["count.index-block"].c_str(), cache_status["bytes.index-block"].c_str(), cache_status["percent.index-block"].c_str(),
             cache_status["count.filter-block"].c_str(), cache_status["bytes.filter-block"].c_str(), cache_status["percent.filter-block"].c_str(),
             cache_status["count.misc"].c_str(), cache_status["bytes.misc"].c_str(), cache_status["percent.misc"].c_str());
      printf("Block cache %s capacity: %s\n", cache_status["id"].c_str(), cache_status["capacity"].c_str());
      std::string cur_size_all_mem_tables;
      db_->GetProperty("rocksdb.cur-size-all-mem-tables", &cur_size_all_mem_tables);
      printf("Current size of all mem tables: %s\n", cur_size_all_mem_tables.c_str());
      if (statistics != nullptr) {
        printf("block cache hit ratio: %lu/%lu = %.2lf%%\n", 
              block_cache_hit, block_cache_hit+block_cache_miss, block_cache_hit_ratio);
        printf("block cache data hit ratio: %lu/%lu = %.2lf%%\n", 
              block_cache_data_hit, block_cache_data_hit+block_cache_data_miss, block_cache_data_hit_ratio);
        printf("block cache compression dict hit ratio: %lu/%lu = %.2lf%%\n", 
              block_cache_compression_dict_hit, block_cache_compression_dict_hit+block_cache_compression_dict_miss, block_cache_compression_dict_hit_ratio);
        printf("memtable hit ratio: %lu/%lu = %.2lf%%\n", 
              memtable_hit, memtable_hit+memtable_miss, memtable_hit_ratio);
        printf("block cache add: %lu\n", statistics->getTickerCount(rocksdb::BLOCK_CACHE_ADD));
        printf("block cache data add: %lu\n", statistics->getTickerCount(rocksdb::BLOCK_CACHE_DATA_ADD));
        // printf("%s\n", statistics->getHistogramString(rocksdb::FLUSH_TIME).c_str());
        // printf("%s\n", statistics->getHistogramString(rocksdb::COMPACTION_TIME).c_str());
        
        // printf("%s\n", statistics->getHistogramString(rocksdb::DB_WRITE).c_str());
        // printf("%s\n", statistics->getHistogramString(rocksdb::READ_BLOCK_COMPACTION_MICROS).c_str());
        // printf("%s\n", statistics->getHistogramString(rocksdb::READ_BLOCK_GET_MICROS).c_str());
        // printf("%s\n", statistics->getHistogramString(rocksdb::WRITE_STALL).c_str());
        // printf("%s\n", statistics->getHistogramString(rocksdb::COMPACTION_TIME).c_str());
      }
    }
    // try to return stats
    if (status_map != nullptr) {
      // block cache
      (*status_map)["block_cache_data_percent"] = cache_status["percent.data-block"];
      if (statistics != nullptr) {
        (*status_map)["block_cache_hit"] = std::to_string(block_cache_hit);
        (*status_map)["block_cache_miss"] = std::to_string(block_cache_miss);
        (*status_map)["block_cache_hit_ratio"] = std::to_string(block_cache_hit_ratio);
        (*status_map)["block_cache_data_hit"] = std::to_string(block_cache_data_hit);
        (*status_map)["block_cache_data_miss"] = std::to_string(block_cache_data_miss);
        (*status_map)["block_cache_data_hit_ratio"] = std::to_string(block_cache_data_hit_ratio);
      }
      // memtable
      if (statistics != nullptr) {
        (*status_map)["memtable_hit"] = std::to_string(memtable_hit);
        (*status_map)["memtable_miss"] = std::to_string(memtable_miss);
        (*status_map)["memtable_hit_ratio"] = std::to_string(memtable_hit_ratio);
      }
    }
    // try to reset stats
    if (statistics != nullptr and reset_stats) {
      statistics->Reset();
      printf("Reset statistics.\n");
    }
  }

 private:
  enum RocksFormat {
    kSingleRow,
  };
  RocksFormat format_;

  void GetOptions(const utils::Properties &props, rocksdb::Options *opt,
                  std::vector<rocksdb::ColumnFamilyDescriptor> *cf_descs);
  static void SerializeRow(const std::vector<Field> &values, std::string &data);
  static void DeserializeRowFilter(std::vector<Field> &values, const char *p, const char *lim,
                                   const std::vector<std::string> &fields);
  static void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                                   const std::vector<std::string> &fields);
  static void DeserializeRow(std::vector<Field> &values, const char *p, const char *lim);
  static void DeserializeRow(std::vector<Field> &values, const std::string &data);

  Status ReadSingle(const std::string &table, const std::string &key,
                    const std::vector<std::string> *fields, std::vector<Field> &result);
  Status ScanSingle(const std::string &table, const std::string &key, int len,
                    const std::vector<std::string> *fields,
                    std::vector<std::vector<Field>> &result);
  Status UpdateSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status MergeSingle(const std::string &table, const std::string &key,
                     std::vector<Field> &values);
  Status InsertSingle(const std::string &table, const std::string &key,
                      std::vector<Field> &values);
  Status DeleteSingle(const std::string &table, const std::string &key);

  Status (RocksdbDB::*method_read_)(const std::string &, const std:: string &,
                                    const std::vector<std::string> *, std::vector<Field> &);
  Status (RocksdbDB::*method_scan_)(const std::string &, const std::string &,
                                    int, const std::vector<std::string> *,
                                    std::vector<std::vector<Field>> &);
  Status (RocksdbDB::*method_update_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (RocksdbDB::*method_insert_)(const std::string &, const std::string &,
                                      std::vector<Field> &);
  Status (RocksdbDB::*method_delete_)(const std::string &, const std::string &);

  int fieldcount_;

  static rocksdb::DB *db_;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewRocksdbDB();

} // ycsbc

#endif // YCSB_C_ROCKSDB_DB_H_

