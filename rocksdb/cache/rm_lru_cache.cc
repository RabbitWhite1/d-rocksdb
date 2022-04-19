//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/rm_lru_cache.h"

#include <cassert>
#include <cstdint>
#include <cstdio>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

RMLRUHandleTable::RMLRUHandleTable(int max_upper_hash_bits)
    : length_bits_(/* historical starting size*/ 4),
      list_(new RMLRUHandle* [size_t{1} << length_bits_] {}),
      elems_(0),
      max_length_bits_(max_upper_hash_bits) {}

RMLRUHandleTable::~RMLRUHandleTable() {
  ApplyToEntriesRange(
      [](RMLRUHandle* h) {
        if (!h->HasRefs()) {
          h->Free();
        }
      },
      0, uint32_t{1} << length_bits_);
}

RMLRUHandle* RMLRUHandleTable:: Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

RMLRUHandle* RMLRUHandleTable::Insert(RMLRUHandle* h) {
  RMLRUHandle** ptr = FindPointer(h->key(), h->hash);
  RMLRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if ((elems_ >> length_bits_) > 0) {  // elems_ >= length
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
  }
  return old;
}

RMLRUHandle* RMLRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  RMLRUHandle** ptr = FindPointer(key, hash);
  RMLRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

RMLRUHandle** RMLRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  RMLRUHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void RMLRUHandleTable::Resize() {
  if (length_bits_ >= max_length_bits_) {
    // Due to reaching limit of hash information, if we made the table
    // bigger, we would allocate more addresses but only the same
    // number would be used.
    return;
  }
  if (length_bits_ >= 31) {
    // Avoid undefined behavior shifting uint32_t by 32
    return;
  }

  uint32_t old_length = uint32_t{1} << length_bits_;
  int new_length_bits = length_bits_ + 1;
  std::unique_ptr<RMLRUHandle* []> new_list {
    new RMLRUHandle* [size_t{1} << new_length_bits] {}
  };
  uint32_t count = 0;
  for (uint32_t i = 0; i < old_length; i++) {
    RMLRUHandle* h = list_[i];
    while (h != nullptr) {
      RMLRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      RMLRUHandle** ptr = &new_list[hash >> (32 - new_length_bits)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  assert(elems_ == count);
  list_ = std::move(new_list);
  length_bits_ = new_length_bits;
}

RMLRUCacheShard::RMLRUCacheShard(
    size_t capacity, bool strict_capacity_limit, double high_pri_pool_ratio,
    bool use_adaptive_mutex, CacheMetadataChargePolicy metadata_charge_policy,
    int max_upper_hash_bits,
    const std::shared_ptr<SecondaryCache>& secondary_cache)
    : capacity_(0),
      high_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      table_(max_upper_hash_bits),
      usage_(0),
      rm_lru_usage_(0),
      mutex_(use_adaptive_mutex),
      secondary_cache_(secondary_cache) {
  set_metadata_charge_policy(metadata_charge_policy);
  // Make empty circular linked list
  rm_lru_.next = &rm_lru_;
  rm_lru_.prev = &rm_lru_;
  rm_lru_low_pri_ = &rm_lru_;
  SetCapacity(capacity);
}

void RMLRUCacheShard::EraseUnRefEntries() {
  autovector<RMLRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    while (rm_lru_.next != &rm_lru_) {
      RMLRUHandle* old = rm_lru_.next;
      // RMLRU list contains only elements which can be evicted
      assert(old->InCache() && !old->HasRefs());
      RMLRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      size_t total_charge = old->CalcTotalCharge(metadata_charge_policy_);
      assert(usage_ >= total_charge);
      usage_ -= total_charge;
      last_reference_list.push_back(old);
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void RMLRUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, void* value, size_t charge,
                             DeleterFn deleter)>& callback,
    uint32_t average_entries_per_lock, uint32_t* state) {
  // The state is essentially going to be the starting hash, which works
  // nicely even if we resize between calls because we use upper-most
  // hash bits for table indexes.
  MutexLock l(&mutex_);
  uint32_t length_bits = table_.GetLengthBits();
  uint32_t length = uint32_t{1} << length_bits;

  assert(average_entries_per_lock > 0);
  // Assuming we are called with same average_entries_per_lock repeatedly,
  // this simplifies some logic (index_end will not overflow)
  assert(average_entries_per_lock < length || *state == 0);

  uint32_t index_begin = *state >> (32 - length_bits);
  uint32_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end
    index_end = length;
    *state = UINT32_MAX;
  } else {
    *state = index_end << (32 - length_bits);
  }

  table_.ApplyToEntriesRange(
      [callback](RMLRUHandle* h) {
        DeleterFn deleter = h->IsSecondaryCacheCompatible()
                                ? h->info_.helper->del_cb
                                : h->info_.deleter;
        callback(h->key(), h->value, h->charge, deleter);
      },
      index_begin, index_end);
}

void RMLRUCacheShard::TEST_GetRMLRUList(RMLRUHandle** rm_lru, RMLRUHandle** rm_lru_low_pri) {
  MutexLock l(&mutex_);
  *rm_lru = &rm_lru_;
  *rm_lru_low_pri = rm_lru_low_pri_;
}

size_t RMLRUCacheShard::TEST_GetRMLRUSize() {
  MutexLock l(&mutex_);
  RMLRUHandle* rm_lru_handle = rm_lru_.next;
  size_t rm_lru_size = 0;
  while (rm_lru_handle != &rm_lru_) {
    rm_lru_size++;
    rm_lru_handle = rm_lru_handle->next;
  }
  return rm_lru_size;
}

double RMLRUCacheShard::GetHighPriPoolRatio() {
  MutexLock l(&mutex_);
  return high_pri_pool_ratio_;
}

void RMLRUCacheShard::RMLRU_Remove(RMLRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (rm_lru_low_pri_ == e) {
    rm_lru_low_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  assert(rm_lru_usage_ >= total_charge);
  rm_lru_usage_ -= total_charge;
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void RMLRUCacheShard::RMLRU_Insert(RMLRUHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of RMLRU list.
    e->next = &rm_lru_;
    e->prev = rm_lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    high_pri_pool_usage_ += total_charge;
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of RMLRU list.
    e->next = rm_lru_low_pri_->next;
    e->prev = rm_lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    rm_lru_low_pri_ = e;
  }
  rm_lru_usage_ += total_charge;
}

void RMLRUCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    rm_lru_low_pri_ = rm_lru_low_pri_->next;
    assert(rm_lru_low_pri_ != &rm_lru_);
    rm_lru_low_pri_->SetInHighPriPool(false);
    size_t total_charge =
        rm_lru_low_pri_->CalcTotalCharge(metadata_charge_policy_);
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void RMLRUCacheShard::EvictFromRMLRU(size_t charge,
                                 autovector<RMLRUHandle*>* deleted) {
  while ((usage_ + charge) > capacity_ && rm_lru_.next != &rm_lru_) {
    RMLRUHandle* old = rm_lru_.next;
    // RMLRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    RMLRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    size_t old_total_charge = old->CalcTotalCharge(metadata_charge_policy_);
    assert(usage_ >= old_total_charge);
    usage_ -= old_total_charge;
    deleted->push_back(old);
  }
}

void RMLRUCacheShard::SetCapacity(size_t capacity) {
  autovector<RMLRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    EvictFromRMLRU(0, &last_reference_list);
  }

  // Try to insert the evicted entries into tiered cache
  // Free the entries outside of mutex for performance reasons
  for (auto entry : last_reference_list) {
    if (secondary_cache_ && entry->IsSecondaryCacheCompatible() &&
        !entry->IsPromoted()) {
      secondary_cache_->Insert(entry->key(), entry->value, entry->info_.helper)
          .PermitUncheckedError();
    }
    entry->Free();
  }
}

void RMLRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  MutexLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status RMLRUCacheShard::InsertItem(RMLRUHandle* e, Cache::Handle** handle,
                                 bool free_handle_on_fail) {
  Status s = Status::OK();
  autovector<RMLRUHandle*> last_reference_list;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);

  {
    MutexLock l(&mutex_);

    // Free the space following strict RMLRU policy until enough space
    // is freed or the rm_lru list is empty
    EvictFromRMLRU(total_charge, &last_reference_list);

    if ((usage_ + total_charge) > capacity_ &&
        (strict_capacity_limit_ || handle == nullptr)) {
      e->SetInCache(false);
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(e);
      } else {
        if (free_handle_on_fail) {
          delete[] reinterpret_cast<char*>(e);
          *handle = nullptr;
        }
        s = Status::Incomplete("Insert failed due to RMLRU cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      RMLRUHandle* old = table_.Insert(e);
      usage_ += total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        if (!old->HasRefs()) {
          // old is on RMLRU because it's in cache and its reference count is 0
          RMLRU_Remove(old);
          size_t old_total_charge =
              old->CalcTotalCharge(metadata_charge_policy_);
          assert(usage_ >= old_total_charge);
          usage_ -= old_total_charge;
          last_reference_list.push_back(old);
        }
      }
      if (handle == nullptr) {
        RMLRU_Insert(e);
      } else {
        // If caller already holds a ref, no need to take one here
        if (!e->HasRefs()) {
          e->Ref();
        }
        *handle = reinterpret_cast<Cache::Handle*>(e);
      }
    }
  }

  // Try to insert the evicted entries into the secondary cache
  // Free the entries here outside of mutex for performance reasons
  for (auto entry : last_reference_list) {
    if (secondary_cache_ && entry->IsSecondaryCacheCompatible() &&
        !entry->IsPromoted()) {
      secondary_cache_->Insert(entry->key(), entry->value, entry->info_.helper)
          .PermitUncheckedError();
    }
    entry->Free();
  }

  return s;
}

void RMLRUCacheShard::Promote(RMLRUHandle* e) {
  SecondaryCacheResultHandle* secondary_handle = e->sec_handle;

  assert(secondary_handle->IsReady());
  e->SetIncomplete(false);
  e->SetInCache(true);
  e->SetPromoted(true);
  e->value = secondary_handle->Value();
  e->charge = secondary_handle->Size();
  delete secondary_handle;

  // This call could fail if the cache is over capacity and
  // strict_capacity_limit_ is true. In such a case, we don't want
  // InsertItem() to free the handle, since the item is already in memory
  // and the caller will most likely just read from disk if we erase it here.
  if (e->value) {
    Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(e);
    Status s = InsertItem(e, &handle, /*free_handle_on_fail=*/false);
    if (!s.ok()) {
      // Item is in memory, but not accounted against the cache capacity.
      // When the handle is released, the item should get deleted
      assert(!e->InCache());
    }
  } else {
    // Since the secondary cache lookup failed, mark the item as not in cache
    // Don't charge the cache as its only metadata that'll shortly be released
    MutexLock l(&mutex_);
    e->charge = 0;
    e->SetInCache(false);
  }
}

Cache::Handle* RMLRUCacheShard::Lookup(
    const Slice& key, uint32_t hash,
    const ShardedCache::CacheItemHelper* helper,
    const ShardedCache::CreateCallback& create_cb, Cache::Priority priority,
    bool wait, Statistics* stats) {
  RMLRUHandle* e = nullptr;
  {
    MutexLock l(&mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      if (!e->HasRefs()) {
        // The entry is in RMLRU since it's in hash and has no external references
        RMLRU_Remove(e);
      }
      e->Ref();
      e->SetHit();
    }
  }

  // If handle table lookup failed, then allocate a handle outside the
  // mutex if we're going to lookup in the secondary cache
  // Only support synchronous for now
  // TODO: Support asynchronous lookup in secondary cache
  if (!e && secondary_cache_ && helper && helper->saveto_cb) {
    // For objects from the secondary cache, we expect the caller to provide
    // a way to create/delete the primary cache object. The only case where
    // a deleter would not be required is for dummy entries inserted for
    // accounting purposes, which we won't demote to the secondary cache
    // anyway.
    assert(create_cb && helper->del_cb);
    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle =
        secondary_cache_->Lookup(key, create_cb, wait);
    if (secondary_handle != nullptr) {
      e = reinterpret_cast<RMLRUHandle*>(
          new char[sizeof(RMLRUHandle) - 1 + key.size()]);

      e->flags = 0;
      e->SetSecondaryCacheCompatible(true);
      e->info_.helper = helper;
      e->key_length = key.size();
      e->hash = hash;
      e->refs = 0;
      e->next = e->prev = nullptr;
      e->SetPriority(priority);
      memcpy(e->key_data, key.data(), key.size());
      e->value = nullptr;
      e->sec_handle = secondary_handle.release();
      e->Ref();

      if (wait) {
        Promote(e);
        if (!e->value) {
          // The secondary cache returned a handle, but the lookup failed
          e->Unref();
          e->Free();
          e = nullptr;
        } else {
          PERF_COUNTER_ADD(secondary_cache_hit_count, 1);
          RecordTick(stats, SECONDARY_CACHE_HITS);
        }
      } else {
        // If wait is false, we always return a handle and let the caller
        // release the handle after checking for success or failure
        e->SetIncomplete(true);
        // This may be slightly inaccurate, if the lookup eventually fails.
        // But the probability is very low.
        PERF_COUNTER_ADD(secondary_cache_hit_count, 1);
        RecordTick(stats, SECONDARY_CACHE_HITS);
      }
    }
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

bool RMLRUCacheShard::Ref(Cache::Handle* h) {
  RMLRUHandle* e = reinterpret_cast<RMLRUHandle*>(h);
  MutexLock l(&mutex_);
  // To create another reference - entry must be already externally referenced
  assert(e->HasRefs());
  e->Ref();
  return true;
}

void RMLRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  MutexLock l(&mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

bool RMLRUCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  if (handle == nullptr) {
    return false;
  }
  RMLRUHandle* e = reinterpret_cast<RMLRUHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    last_reference = e->Unref();
    if (last_reference && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (usage_ > capacity_ || force_erase) {
        // The RMLRU list must be empty since the cache is full
        assert(rm_lru_.next == &rm_lru_ || force_erase);
        // Take this opportunity and remove the item
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
      } else {
        // Put the item back on the RMLRU list, and don't free it
        RMLRU_Insert(e);
        last_reference = false;
      }
    }
    // If it was the last reference, and the entry is either not secondary
    // cache compatible (i.e a dummy entry for accounting), or is secondary
    // cache compatible and has a non-null value, then decrement the cache
    // usage. If value is null in the latter case, taht means the lookup
    // failed and we didn't charge the cache.
    if (last_reference && (!e->IsSecondaryCacheCompatible() || e->value)) {
      size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
      assert(usage_ >= total_charge);
      usage_ -= total_charge;
    }
  }

  // Free the entry here outside of mutex for performance reasons
  if (last_reference) {
    e->Free();
  }
  return last_reference;
}

Status RMLRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge,
                             void (*deleter)(const Slice& key, void* value),
                             const Cache::CacheItemHelper* helper,
                             Cache::Handle** handle, Cache::Priority priority) {
  // Allocate the memory here outside of the mutex
  // If the cache is full, we'll have to release it
  // It shouldn't happen very often though.
  RMLRUHandle* e = reinterpret_cast<RMLRUHandle*>(
      new char[sizeof(RMLRUHandle) - 1 + key.size()]);

  e->value = value;
  e->flags = 0;
  if (helper) {
    e->SetSecondaryCacheCompatible(true);
    e->info_.helper = helper;
  } else {
#ifdef __SANITIZE_THREAD__
    e->is_secondary_cache_compatible_for_tsan = false;
#endif  // __SANITIZE_THREAD__
    e->info_.deleter = deleter;
  }
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 0;
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetPriority(priority);
  memcpy(e->key_data, key.data(), key.size());

  return InsertItem(e, handle, /* free_handle_on_fail */ true);
}

void RMLRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  RMLRUHandle* e;
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      e->SetInCache(false);
      if (!e->HasRefs()) {
        // The entry is in RMLRU since it's in hash and has no external references
        RMLRU_Remove(e);
        size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
        assert(usage_ >= total_charge);
        usage_ -= total_charge;
        last_reference = true;
      }
    }
  }

  // Free the entry here outside of mutex for performance reasons
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    e->Free();
  }
}

bool RMLRUCacheShard::IsReady(Cache::Handle* handle) {
  RMLRUHandle* e = reinterpret_cast<RMLRUHandle*>(handle);
  MutexLock l(&mutex_);
  bool ready = true;
  if (e->IsPending()) {
    assert(secondary_cache_);
    assert(e->sec_handle);
    ready = e->sec_handle->IsReady();
  }
  return ready;
}

size_t RMLRUCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t RMLRUCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= rm_lru_usage_);
  return usage_ - rm_lru_usage_;
}

std::string RMLRUCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

RMLRUCache::RMLRUCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   std::shared_ptr<MemoryAllocator> allocator,
                   bool use_adaptive_mutex,
                   CacheMetadataChargePolicy metadata_charge_policy,
                   const std::shared_ptr<SecondaryCache>& secondary_cache)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<RMLRUCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(RMLRUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  printf("RMLRUCache num_shards: %d, per_shard: %lu\n", num_shards_, per_shard);
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i]) RMLRUCacheShard(
        per_shard, strict_capacity_limit, high_pri_pool_ratio,
        use_adaptive_mutex, metadata_charge_policy,
        /* max_upper_hash_bits */ 32 - num_shard_bits, secondary_cache);
  }
  secondary_cache_ = secondary_cache;
}

RMLRUCache::~RMLRUCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~RMLRUCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* RMLRUCache::GetShard(uint32_t shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* RMLRUCache::GetShard(uint32_t shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* RMLRUCache::Value(Handle* handle) {
  return reinterpret_cast<const RMLRUHandle*>(handle)->value;
}

size_t RMLRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const RMLRUHandle*>(handle)->charge;
}

Cache::DeleterFn RMLRUCache::GetDeleter(Handle* handle) const {
  auto h = reinterpret_cast<const RMLRUHandle*>(handle);
  if (h->IsSecondaryCacheCompatible()) {
    return h->info_.helper->del_cb;
  } else {
    return h->info_.deleter;
  }
}

uint32_t RMLRUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const RMLRUHandle*>(handle)->hash;
}

void RMLRUCache::DisownData() {
  // Leak data only if that won't generate an ASAN/valgrind warning
  if (!kMustFreeHeapAllocations) {
    shards_ = nullptr;
    num_shards_ = 0;
  }
}

size_t RMLRUCache::TEST_GetRMLRUSize() {
  size_t rm_lru_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    rm_lru_size_of_all_shards += shards_[i].TEST_GetRMLRUSize();
  }
  return rm_lru_size_of_all_shards;
}

double RMLRUCache::GetHighPriPoolRatio() {
  double result = 0.0;
  if (num_shards_ > 0) {
    result = shards_[0].GetHighPriPoolRatio();
  }
  return result;
}

void RMLRUCache::WaitAll(std::vector<Handle*>& handles) {
  if (secondary_cache_) {
    std::vector<SecondaryCacheResultHandle*> sec_handles;
    sec_handles.reserve(handles.size());
    for (Handle* handle : handles) {
      if (!handle) {
        continue;
      }
      RMLRUHandle* rm_lru_handle = reinterpret_cast<RMLRUHandle*>(handle);
      if (!rm_lru_handle->IsPending()) {
        continue;
      }
      sec_handles.emplace_back(rm_lru_handle->sec_handle);
    }
    secondary_cache_->WaitAll(sec_handles);
    for (Handle* handle : handles) {
      if (!handle) {
        continue;
      }
      RMLRUHandle* rm_lru_handle = reinterpret_cast<RMLRUHandle*>(handle);
      if (!rm_lru_handle->IsPending()) {
        continue;
      }
      uint32_t hash = GetHash(handle);
      RMLRUCacheShard* shard = static_cast<RMLRUCacheShard*>(GetShard(Shard(hash)));
      shard->Promote(rm_lru_handle);
    }
  }
}

std::shared_ptr<Cache> NewRMLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    const std::shared_ptr<SecondaryCache>& secondary_cache) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<RMLRUCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      std::move(memory_allocator), use_adaptive_mutex, metadata_charge_policy,
      secondary_cache);
}

std::shared_ptr<Cache> NewRMLRUCache(const RMLRUCacheOptions& cache_opts) {
  return NewRMLRUCache(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.high_pri_pool_ratio,
      cache_opts.memory_allocator, cache_opts.use_adaptive_mutex,
      cache_opts.metadata_charge_policy, cache_opts.secondary_cache);
}

std::shared_ptr<Cache> NewRMLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy) {
  return NewRMLRUCache(capacity, num_shard_bits, strict_capacity_limit,
                     high_pri_pool_ratio, memory_allocator, use_adaptive_mutex,
                     metadata_charge_policy, nullptr);
}
}  // namespace ROCKSDB_NAMESPACE
