//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <memory>
#include <string>

#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/secondary_cache.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

// RMLRU cache implementation. This class is not thread-safe.

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in a hash table. Some elements
// are also stored on RMLRU list.
//
// RMLRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//    In that case the entry is *not* in the RMLRU list
//    (refs >= 1 && in_cache == true)
// 2. Not referenced externally AND in hash table.
//    In that case the entry is in the RMLRU list and can be freed.
//    (refs == 0 && in_cache == true)
// 3. Referenced externally AND not in hash table.
//    In that case the entry is not in the RMLRU list and not in hash table.
//    The entry can be freed when refs becomes 0.
//    (refs >= 1 && in_cache == false)
//
// All newly created RMLRUHandles are in state 1. If you call
// RMLRUCacheShard::Release on entry in state 1, it will go into state 2.
// To move from state 1 to state 3, either call RMLRUCacheShard::Erase or
// RMLRUCacheShard::Insert with the same key (but possibly different value).
// To move from state 2 to state 1, use RMLRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful RMLRUCacheShard::Lookup/RMLRUCacheShard::Insert have a
// matching RMLRUCache::Release (to move into state 2) or RMLRUCacheShard::Erase
// (to move into state 3).

struct RMLRUHandle {
  void* value;
  union Info {
    Info() {}
    ~Info() {}
    Cache::DeleterFn deleter;
    const ShardedCache::CacheItemHelper* helper;
  } info_;
  // An entry is not added to the RMLRUHandleTable until the secondary cache
  // lookup is complete, so its safe to have this union.
  union {
    RMLRUHandle* next_hash;
    SecondaryCacheResultHandle* sec_handle;
  };
  RMLRUHandle* next;
  RMLRUHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  // The hash of key(). Used for fast sharding and comparisons.
  uint32_t hash;
  // The number of external refs to this entry. The cache itself is not counted.
  uint32_t refs;

  enum Flags : uint8_t {
    // Whether this entry is referenced by the hash table.
    IN_CACHE = (1 << 0),
    // Whether this entry is high priority entry.
    IS_HIGH_PRI = (1 << 1),
    // Whether this entry is in high-pri pool.
    IN_HIGH_PRI_POOL = (1 << 2),
    // Whether this entry has had any lookups (hits).
    HAS_HIT = (1 << 3),
    // Can this be inserted into the secondary cache
    IS_SECONDARY_CACHE_COMPATIBLE = (1 << 4),
    // Is the handle still being read from a lower tier
    IS_PENDING = (1 << 5),
    // Has the item been promoted from a lower tier
    IS_PROMOTED = (1 << 6),
    // Is the data of the handle in local
    IS_LOCAL = (1 << 7),
  };

  uint8_t flags;

#ifdef __SANITIZE_THREAD__
  // TSAN can report a false data race on flags, where one thread is writing
  // to one of the mutable bits and another thread is reading this immutable
  // bit. So precisely suppress that TSAN warning, we separate out this bit
  // during TSAN runs.
  bool is_secondary_cache_compatible_for_tsan;
#endif  // __SANITIZE_THREAD__

  // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
  char key_data[1];

  Slice key() const { return Slice(key_data, key_length); }

  // Increase the reference count by 1.
  void Ref() { refs++; }

  // Just reduce the reference count by 1. Return true if it was last reference.
  bool Unref() {
    assert(refs > 0);
    refs--;
    return refs == 0;
  }

  // Return true if there are external refs, false otherwise.
  bool HasRefs() const { return refs > 0; }

  bool InCache() const { return flags & IN_CACHE; }
  bool IsHighPri() const { return flags & IS_HIGH_PRI; }
  bool InHighPriPool() const { return flags & IN_HIGH_PRI_POOL; }
  bool HasHit() const { return flags & HAS_HIT; }
  bool IsSecondaryCacheCompatible() const {
#ifdef __SANITIZE_THREAD__
    return is_secondary_cache_compatible_for_tsan;
#else
    return flags & IS_SECONDARY_CACHE_COMPATIBLE;
#endif  // __SANITIZE_THREAD__
  }
  bool IsPending() const { return flags & IS_PENDING; }
  bool IsPromoted() const { return flags & IS_PROMOTED; }
  bool IsLocal() const { return flags & IS_LOCAL; }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      flags |= IN_CACHE;
    } else {
      flags &= ~IN_CACHE;
    }
  }

  void SetPriority(Cache::Priority priority) {
    if (priority == Cache::Priority::HIGH) {
      flags |= IS_HIGH_PRI;
    } else {
      flags &= ~IS_HIGH_PRI;
    }
  }

  void SetInHighPriPool(bool in_high_pri_pool) {
    if (in_high_pri_pool) {
      flags |= IN_HIGH_PRI_POOL;
    } else {
      flags &= ~IN_HIGH_PRI_POOL;
    }
  }

  void SetHit() { flags |= HAS_HIT; }

  void SetSecondaryCacheCompatible(bool compat) {
    if (compat) {
      flags |= IS_SECONDARY_CACHE_COMPATIBLE;
    } else {
      flags &= ~IS_SECONDARY_CACHE_COMPATIBLE;
    }
#ifdef __SANITIZE_THREAD__
    is_secondary_cache_compatible_for_tsan = compat;
#endif  // __SANITIZE_THREAD__
  }

  void SetIncomplete(bool incomp) {
    if (incomp) {
      flags |= IS_PENDING;
    } else {
      flags &= ~IS_PENDING;
    }
  }

  void SetPromoted(bool promoted) {
    if (promoted) {
      flags |= IS_PROMOTED;
    } else {
      flags &= ~IS_PROMOTED;
    }
  }

  void SetDataLocation(bool is_local) {
    if (is_local) {
      flags |= IS_LOCAL;
    } else {
      flags &= ~IS_LOCAL;
    }
  }

  void Free() {
    assert(refs == 0);
#ifdef __SANITIZE_THREAD__
    // Here we can safely assert they are the same without a data race reported
    assert(((flags & IS_SECONDARY_CACHE_COMPATIBLE) != 0) ==
           is_secondary_cache_compatible_for_tsan);
#endif  // __SANITIZE_THREAD__
    if (!IsSecondaryCacheCompatible() && info_.deleter) {
      (*info_.deleter)(key(), value);
    } else if (IsSecondaryCacheCompatible()) {
      if (IsPending()) {
        assert(sec_handle != nullptr);
        SecondaryCacheResultHandle* tmp_sec_handle = sec_handle;
        tmp_sec_handle->Wait();
        value = tmp_sec_handle->Value();
        delete tmp_sec_handle;
      }
      if (value) {
        (*info_.helper->del_cb)(key(), value);
      }
    }
    delete[] reinterpret_cast<char*>(this);
  }

  // Calculate the memory usage by metadata
  inline size_t CalcTotalCharge(
      CacheMetadataChargePolicy metadata_charge_policy) {
    size_t meta_charge = 0;
    if (metadata_charge_policy == kFullChargeCacheMetadata) {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      meta_charge += malloc_usable_size(static_cast<void*>(this));
#else
      // This is the size that is used when a new handle is created
      meta_charge += sizeof(RMLRUHandle) - 1 + key_length;
#endif
    }
    return charge + meta_charge;
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class RMLRUHandleTable {
 public:
  // If the table uses more hash bits than `max_upper_hash_bits`,
  // it will eat into the bits used for sharding, which are constant
  // for a given RMLRUHandleTable.
  explicit RMLRUHandleTable(int max_upper_hash_bits);
  ~RMLRUHandleTable();

  RMLRUHandle* Lookup(const Slice& key, uint32_t hash);
  RMLRUHandle* Insert(RMLRUHandle* h);
  RMLRUHandle* Remove(const Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToEntriesRange(T func, uint32_t index_begin, uint32_t index_end) {
    for (uint32_t i = index_begin; i < index_end; i++) {
      RMLRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

  int GetLengthBits() const { return length_bits_; }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  RMLRUHandle** FindPointer(const Slice& key, uint32_t hash);

  void Resize();

  // Number of hash bits (upper because lower bits used for sharding)
  // used for table index. Length == 1 << length_bits_
  int length_bits_;

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  std::unique_ptr<RMLRUHandle*[]> list_;

  // Number of elements currently in the table
  uint32_t elems_;

  // Set from max_upper_hash_bits (see constructor)
  const int max_length_bits_;
};

// A single shard of sharded cache.
class ALIGN_AS(CACHE_LINE_SIZE) RMLRUCacheShard final : public CacheShard {
 public:
  RMLRUCacheShard(size_t capacity, bool strict_capacity_limit,
                double high_pri_pool_ratio, bool use_adaptive_mutex,
                CacheMetadataChargePolicy metadata_charge_policy,
                int max_upper_hash_bits,
                const std::shared_ptr<SecondaryCache>& secondary_cache);
  virtual ~RMLRUCacheShard() override = default;

  // Separate from constructor so caller can easily make an array of RMLRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  virtual void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // Like Cache methods, but with an extra "hash" parameter.
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge, Cache::DeleterFn deleter,
                        Cache::Handle** handle,
                        Cache::Priority priority) override {
    return Insert(key, hash, value, charge, deleter, nullptr, handle, priority);
  }
  virtual Status Insert(const Slice& key, uint32_t hash, void* value,
                        const Cache::CacheItemHelper* helper, size_t charge,
                        Cache::Handle** handle,
                        Cache::Priority priority) override {
    assert(helper);
    return Insert(key, hash, value, charge, nullptr, helper, handle, priority);
  }
  // If helper_cb is null, the values of the following arguments don't
  // matter
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash,
                                const ShardedCache::CacheItemHelper* helper,
                                const ShardedCache::CreateCallback& create_cb,
                                ShardedCache::Priority priority, bool wait,
                                Statistics* stats) override;
  virtual Cache::Handle* Lookup(const Slice& key, uint32_t hash) override {
    return Lookup(key, hash, nullptr, nullptr, Cache::Priority::LOW, true,
                  nullptr);
  }
  virtual bool Release(Cache::Handle* handle, bool /*useful*/,
                       bool force_erase) override {
    return Release(handle, force_erase);
  }
  virtual bool IsReady(Cache::Handle* /*handle*/) override;
  virtual void Wait(Cache::Handle* /*handle*/) override {}
  virtual bool Ref(Cache::Handle* handle) override;
  virtual bool Release(Cache::Handle* handle,
                       bool force_erase = false) override;
  virtual void Erase(const Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToSomeEntries(
      const std::function<void(const Slice& key, void* value, size_t charge,
                               DeleterFn deleter)>& callback,
      uint32_t average_entries_per_lock, uint32_t* state) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;

  void TEST_GetRMLRUList(RMLRUHandle** rm_lru, RMLRUHandle** rm_lru_low_pri);

  //  Retrieves number of elements in RMLRU, for unit test purpose only
  //  not threadsafe
  size_t TEST_GetRMLRUSize();

  //  Retrieves high pri pool ratio
  double GetHighPriPoolRatio();

 private:
  friend class RMLRUCache;
  // Insert an item into the hash table and, if handle is null, insert into
  // the RMLRU list. Older items are evicted as necessary. If the cache is full
  // and free_handle_on_fail is true, the item is deleted and handle is set to.
  Status InsertItem(RMLRUHandle* item, Cache::Handle** handle,
                    bool free_handle_on_fail);
  Status Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
                DeleterFn deleter, const Cache::CacheItemHelper* helper,
                Cache::Handle** handle, Cache::Priority priority);
  // Promote an item looked up from the secondary cache to the RMLRU cache. The
  // item is only inserted into the hash table and not the RMLRU list, and only
  // if the cache is not at full capacity, as is the case during Insert.  The
  // caller should hold a reference on the RMLRUHandle. When the caller releases
  // the last reference, the item is added to the RMLRU list.
  // The item is promoted to the high pri or low pri pool as specified by the
  // caller in Lookup.
  void Promote(RMLRUHandle* e);
  void RMLRU_Remove(RMLRUHandle* e);
  void RMLRU_Insert(RMLRUHandle* e);

  // Overflow the last entry in high-pri pool to low-pri pool until size of
  // high-pri pool is no larger than the size specify by high_pri_pool_pct.
  void MaintainPoolSize();

  // Free some space following strict RMLRU policy until enough space
  // to hold (usage_ + charge) is freed or the rm_lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromRMLRU(size_t charge, autovector<RMLRUHandle*>* deleted);

  // Initialized before use.
  size_t capacity_;

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // Dummy head of RMLRU list.
  // rm_lru.prev is newest entry, rm_lru.next is oldest entry.
  // RMLRU contains items which can be evicted, ie reference only by cache
  RMLRUHandle rm_lru_;

  // Pointer to head of low-pri pool in RMLRU list.
  RMLRUHandle* rm_lru_low_pri_;

  // ------------^^^^^^^^^^^^^-----------
  // Not frequently modified data members
  // ------------------------------------
  //
  // We separate data members that are updated frequently from the ones that
  // are not frequently updated so that they don't share the same cache line
  // which will lead into false cache sharing
  //
  // ------------------------------------
  // Frequently modified data members
  // ------------vvvvvvvvvvvvv-----------
  RMLRUHandleTable table_;

  // Memory size for entries residing in the cache
  size_t usage_;

  // Memory size for entries residing only in the RMLRU list
  size_t rm_lru_usage_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable port::Mutex mutex_;

  std::shared_ptr<SecondaryCache> secondary_cache_;
};

class RMLRUCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache {
 public:
  RMLRUCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
           double high_pri_pool_ratio,
           std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
           bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
           CacheMetadataChargePolicy metadata_charge_policy =
               kDontChargeCacheMetadata,
           const std::shared_ptr<SecondaryCache>& secondary_cache = nullptr);
  virtual ~RMLRUCache();
  virtual const char* Name() const override { return "RMLRUCache"; }
  virtual CacheShard* GetShard(uint32_t shard) override;
  virtual const CacheShard* GetShard(uint32_t shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual DeleterFn GetDeleter(Handle* handle) const override;
  virtual void DisownData() override;
  virtual void WaitAll(std::vector<Handle*>& handles) override;

  //  Retrieves number of elements in RMLRU, for unit test purpose only
  size_t TEST_GetRMLRUSize();
  //  Retrieves high pri pool ratio
  double GetHighPriPoolRatio();

 private:
  RMLRUCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
  std::shared_ptr<SecondaryCache> secondary_cache_;
};

}  // namespace ROCKSDB_NAMESPACE
