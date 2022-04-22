//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/d_lru_cache.h"

#include <cassert>
#include <cstdint>
#include <cstdio>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

DLRUHandleTable::DLRUHandleTable(int max_upper_hash_bits)
    : length_bits_(/* historical starting size*/ 4),
      list_(new DLRUHandle* [size_t{1} << length_bits_] {}),
      elems_(0),
      max_length_bits_(max_upper_hash_bits) {}

DLRUHandleTable::~DLRUHandleTable() {
  ApplyToEntriesRange(
      [](DLRUHandle* h) {
        if (!h->HasRefs()) {
          h->Free();
        }
      },
      0, uint32_t{1} << length_bits_);
}

DLRUHandle* DLRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

DLRUHandle* DLRUHandleTable::Insert(DLRUHandle* h) {
  DLRUHandle** ptr = FindPointer(h->key(), h->hash);
  DLRUHandle* old = *ptr;
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

DLRUHandle* DLRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  DLRUHandle** ptr = FindPointer(key, hash);
  DLRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

DLRUHandle** DLRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  DLRUHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void DLRUHandleTable::Resize() {
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
  std::unique_ptr<DLRUHandle* []> new_list {
    new DLRUHandle* [size_t{1} << new_length_bits] {}
  };
  uint32_t count = 0;
  for (uint32_t i = 0; i < old_length; i++) {
    DLRUHandle* h = list_[i];
    while (h != nullptr) {
      DLRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      DLRUHandle** ptr = &new_list[hash >> (32 - new_length_bits)];
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

DLRUCacheShard::DLRUCacheShard(
    size_t capacity, bool strict_capacity_limit, double high_pri_pool_ratio,
    double rm_ratio, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy, int max_upper_hash_bits,
    const std::shared_ptr<RemoteMemory>& remote_memory)
    : capacity_(0),
      high_pri_pool_usage_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0),
      rm_ratio_(rm_ratio),
      table_(max_upper_hash_bits),
      usage_(0),
      lm_lru_usage_(0),
      mutex_(use_adaptive_mutex),
      remote_memory_(remote_memory) {
  set_metadata_charge_policy(metadata_charge_policy);
  // Make empty circular linked list
  lm_lru_.next = &lm_lru_;
  lm_lru_.prev = &lm_lru_;
  lm_lru_low_pri_ = &lm_lru_;
  rm_lru_.next = &rm_lru_;
  rm_lru_.prev = &rm_lru_;
  SetCapacity(capacity);
}

void DLRUCacheShard::EraseUnRefEntries() {
  autovector<DLRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    while (lm_lru_.next != &lm_lru_) {
      DLRUHandle* old = lm_lru_.next;
      // DLRU list contains only elements which can be evicted
      assert(old->InCache() && !old->HasRefs());
      LMLRU_Remove(old);
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

void DLRUCacheShard::ApplyToSomeEntries(
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
      [callback](DLRUHandle* h) {
        DeleterFn deleter = h->IsSecondaryCacheCompatible()
                                ? h->info_.helper->del_cb
                                : h->info_.deleter;
        callback(h->key(), h->value, h->charge, deleter);
      },
      index_begin, index_end);
}

void DLRUCacheShard::TEST_GetDLRUList(DLRUHandle** rm_lru,
                                      DLRUHandle** lm_lru_low_pri) {
  MutexLock l(&mutex_);
  *rm_lru = &lm_lru_;
  *lm_lru_low_pri = lm_lru_low_pri_;
}

size_t DLRUCacheShard::TEST_GetDLRUSize() {
  MutexLock l(&mutex_);
  DLRUHandle* lm_lru_handle = lm_lru_.next;
  size_t lm_lru_size = 0;
  while (lm_lru_handle != &lm_lru_) {
    lm_lru_size++;
    lm_lru_handle = lm_lru_handle->next;
  }
  return lm_lru_size;
}

double DLRUCacheShard::GetHighPriPoolRatio() {
  MutexLock l(&mutex_);
  return high_pri_pool_ratio_;
}

void DLRUCacheShard::LMLRU_Remove(DLRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (lm_lru_low_pri_ == e) {
    lm_lru_low_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  assert(lm_lru_usage_ >= total_charge);
  lm_lru_usage_ -= total_charge;
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void DLRUCacheShard::LMLRU_Insert(DLRUHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of DLRU list.
    e->next = &lm_lru_;
    e->prev = lm_lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    high_pri_pool_usage_ += total_charge;
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of DLRU
    // list.
    e->next = lm_lru_low_pri_->next;
    e->prev = lm_lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    lm_lru_low_pri_ = e;
  }
  lm_lru_usage_ += total_charge;
}

void DLRUCacheShard::RMLRU_Remove(DLRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  assert(rm_lru_usage_ >= total_charge);
  rm_lru_usage_ -= total_charge;
}

void DLRUCacheShard::RMLRU_Insert(DLRUHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  // Inset "e" to head of RMLRU list.
  e->next = &rm_lru_;
  e->prev = rm_lru_.prev;
  e->prev->next = e;
  e->next->prev = e;
  e->SetInHighPriPool(false);
  rm_lru_usage_ += total_charge;
}

void DLRUCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    lm_lru_low_pri_ = lm_lru_low_pri_->next;
    assert(lm_lru_low_pri_ != &lm_lru_);
    lm_lru_low_pri_->SetInHighPriPool(false);
    size_t total_charge =
        lm_lru_low_pri_->CalcTotalCharge(metadata_charge_policy_);
    assert(high_pri_pool_usage_ >= total_charge);
    high_pri_pool_usage_ -= total_charge;
  }
}

void DLRUCacheShard::EvictFromLMLRU(size_t charge,
                                    autovector<DLRUHandle*>* deleted) {
  while ((usage_ + charge) > capacity_ && lm_lru_.next != &lm_lru_) {
    DLRUHandle* old = lm_lru_.next;
    // LRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    LMLRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    size_t old_total_charge = old->CalcTotalCharge(metadata_charge_policy_);
    assert(usage_ >= old_total_charge);
    usage_ -= old_total_charge;
    deleted->push_back(old);
  }
}

void DLRUCacheShard::EvictFromLMLRUToRMLRU(
    size_t charge, autovector<DLRUHandle*>* evicted_to_rm_list) {
  while ((lm_lru_usage_ + charge) > lm_capacity_ && lm_lru_.next != &lm_lru_) {
    DLRUHandle* old = lm_lru_.next;
    // DLRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    LMLRU_Remove(old);
    RMLRU_Insert(old);
    // size_t old_total_charge = old->CalcTotalCharge(metadata_charge_policy_);
    // evicting from local to remote, so usage didn't change,
    // only when evicting from remote, total usage changes (see EvictFromRMLRU)
    evicted_to_rm_list->push_back(old);
  }
}

void DLRUCacheShard::MoveValueToRM(DLRUHandle* e) {
  assert(e->InCache());
  assert(e->IsLocal());
  assert(remote_memory_);
  assert(e->info_.helper && "if using rm, should use helper with size_cb");
  e->slice_size = e->info_.helper->size_cb(e->value);
  printf("charging rm: %lu\n", e->slice_size);
  uint64_t rm_addr = remote_memory_->rmalloc(e->slice_size);
  remote_memory_->write(rm_addr, e->value, e->slice_size);
  e->FreeValue();
  e->value = (void*)rm_addr;
}

void DLRUCacheShard::FetchFromRM(
    DLRUHandle* e, const ShardedCache::CreateCallback& create_cb) {
  assert(e->InCache());
  assert(!e->IsLocal());
  assert(remote_memory_);
  assert(e->info_.helper && "if using rm, should use helper with size_cb");
  e->slice_size = e->info_.helper->size_cb(e->value);
  uint64_t rm_addr = (uint64_t)e->value;
  std::unique_ptr<char[]> buf_data(new char[e->slice_size]());
  remote_memory_->read(rm_addr, buf_data.get(), e->slice_size);
  Status s = create_cb(buf_data.get(), e->slice_size, &e->value, &e->charge);
  assert(s.ok());
}

void DLRUCacheShard::EvictFromRMLRU(size_t charge,
                                    autovector<DLRUHandle*>* deleted) {
  // TODO: don't need consider charge, because new block is inserted to lm_lru.
  // will remove later.
  while ((rm_lru_usage_ + charge) > rm_capacity_ && rm_lru_.next != &rm_lru_) {
    DLRUHandle* old = rm_lru_.next;
    // DLRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    RMLRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    size_t old_total_charge = old->CalcTotalCharge(metadata_charge_policy_);
    // only when evicting from remote, total usage changes
    assert(usage_ >= old_total_charge);
    usage_ -= old_total_charge;
    deleted->push_back(old);
  }
}

void DLRUCacheShard::SetCapacity(size_t capacity) {
  autovector<DLRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    lm_capacity_ = capacity_ * (1 - rm_ratio_);
    rm_capacity_ = capacity_ * rm_ratio_;
    EvictFromLMLRU(0, &last_reference_list);
  }

  // Try to insert the evicted entries into tiered cache
  // Free the entries outside of mutex for performance reasons
  for (auto entry : last_reference_list) {
    // TODO: maybe evict to remote memory
    entry->Free();
  }
}

void DLRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  MutexLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status DLRUCacheShard::InsertItem(DLRUHandle* e, Cache::Handle** handle,
                                  bool free_handle_on_fail) {
  Status s = Status::OK();
  autovector<DLRUHandle*> evict_to_rm_list;
  autovector<DLRUHandle*> last_reference_list;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  // printf("cache charge size=%lu\n", total_charge);

  {
    MutexLock l(&mutex_);

    // Free the space following strict DLRU policy until enough space
    // is freed or the rm_lru list is empty
    if (remote_memory_) {
      EvictFromLMLRUToRMLRU(total_charge, &evict_to_rm_list);
      EvictFromRMLRU(total_charge, &last_reference_list);
    } else {
      EvictFromLMLRU(total_charge, &last_reference_list);
    }

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
        s = Status::Incomplete("Insert failed due to DLRU cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      DLRUHandle* old = table_.Insert(e);
      usage_ += total_charge;
      if (old != nullptr) {
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        if (!old->HasRefs()) {
          // old is on DLRU because it's in cache and its reference count is 0
          LMLRU_Remove(old);
          size_t old_total_charge =
              old->CalcTotalCharge(metadata_charge_policy_);
          assert(usage_ >= old_total_charge);
          usage_ -= old_total_charge;
          last_reference_list.push_back(old);
        }
      }
      if (handle == nullptr) {
        LMLRU_Insert(e);
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
  for (auto entry : evict_to_rm_list) {
    MoveValueToRM(entry);
  }
  for (auto entry : last_reference_list) {
    entry->Free();
  }

  return s;
}

void DLRUCacheShard::Promote(DLRUHandle* e) {
  // TODO: check whether need this
  printf("should not call this\n");
}

Cache::Handle* DLRUCacheShard::Lookup(
    const Slice& key, uint32_t hash,
    const ShardedCache::CacheItemHelper* helper,
    const ShardedCache::CreateCallback& create_cb, Cache::Priority priority,
    bool wait, Statistics* stats) {
  DLRUHandle* e = nullptr;
  {
    MutexLock l(&mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      if (!e->HasRefs()) {
        // The entry is in DLRU since it's in hash and has no external
        // references
        LMLRU_Remove(e);
      }
      if (!e->IsLocal()) {
        // fetch it from remote memory
        FetchFromRM(e, create_cb);
      }
      e->Ref();
      e->SetHit();
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

bool DLRUCacheShard::Ref(Cache::Handle* h) {
  DLRUHandle* e = reinterpret_cast<DLRUHandle*>(h);
  MutexLock l(&mutex_);
  // To create another reference - entry must be already externally referenced
  assert(e->HasRefs());
  e->Ref();
  return true;
}

void DLRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  MutexLock l(&mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

bool DLRUCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  if (handle == nullptr) {
    return false;
  }
  DLRUHandle* e = reinterpret_cast<DLRUHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    last_reference = e->Unref();
    if (last_reference && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (usage_ > capacity_ || force_erase) {
        // The DLRU list must be empty since the cache is full
        assert(lm_lru_.next == &lm_lru_ || force_erase);
        // Take this opportunity and remove the item
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
      } else {
        // Put the item back on the DLRU list, and don't free it
        LMLRU_Insert(e);
        last_reference = false;
      }
    }
    // If it was the last reference, and the entry is either not secondary
    // cache compatible (i.e a dummy entry for accounting), or is secondary
    // cache compatible and has a non-null value, then decrement the cache
    // usage. If value is null in the latter case, taht means the lookup
    // failed and we didn't charge the cache.
    // TODO: check what to do here
    // if (last_reference && (!e->IsSecondaryCacheCompatible() || e->value)) {
    if (last_reference) {
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

Status DLRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                              size_t charge,
                              void (*deleter)(const Slice& key, void* value),
                              const Cache::CacheItemHelper* helper,
                              Cache::Handle** handle,
                              Cache::Priority priority) {
  // Allocate the memory here outside of the mutex
  // If the cache is full, we'll have to release it
  // It shouldn't happen very often though.
  DLRUHandle* e = reinterpret_cast<DLRUHandle*>(
      new char[sizeof(DLRUHandle) - 1 + key.size()]);

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
  e->slice_size = 0;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = 0;
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetLocal(true);
  e->SetPriority(priority);
  memcpy(e->key_data, key.data(), key.size());

  return InsertItem(e, handle, /* free_handle_on_fail */ true);
}

void DLRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  DLRUHandle* e;
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      e->SetInCache(false);
      if (!e->HasRefs()) {
        // The entry is in DLRU since it's in hash and has no external
        // references
        LMLRU_Remove(e);
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

bool DLRUCacheShard::IsReady(Cache::Handle* handle) {
  // TODO: check whether need this
  printf("should not call this\n");
  return false;
}

size_t DLRUCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t DLRUCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= lm_lru_usage_);
  return usage_ - lm_lru_usage_;
}

std::string DLRUCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

DLRUCache::DLRUCache(size_t capacity, int num_shard_bits,
                     bool strict_capacity_limit, double high_pri_pool_ratio,
                     double rm_ratio,
                     std::shared_ptr<MemoryAllocator> allocator,
                     bool use_adaptive_mutex,
                     CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<DLRUCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(DLRUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  printf("DLRUCache num_shards: %d, per_shard: %lu\n", num_shards_, per_shard);
  remote_memory_ =
      std::make_shared<RemoteMemory>("10.0.0.5", capacity * rm_ratio);
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i]) DLRUCacheShard(
        per_shard, strict_capacity_limit, high_pri_pool_ratio, rm_ratio,
        use_adaptive_mutex, metadata_charge_policy,
        /* max_upper_hash_bits */ 32 - num_shard_bits, remote_memory_);
  }
}

DLRUCache::~DLRUCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~DLRUCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* DLRUCache::GetShard(uint32_t shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* DLRUCache::GetShard(uint32_t shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* DLRUCache::Value(Handle* handle) {
  return reinterpret_cast<const DLRUHandle*>(handle)->value;
}

size_t DLRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const DLRUHandle*>(handle)->charge;
}

Cache::DeleterFn DLRUCache::GetDeleter(Handle* handle) const {
  auto h = reinterpret_cast<const DLRUHandle*>(handle);
  if (h->IsSecondaryCacheCompatible()) {
    return h->info_.helper->del_cb;
  } else {
    return h->info_.deleter;
  }
}

uint32_t DLRUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const DLRUHandle*>(handle)->hash;
}

void DLRUCache::DisownData() {
  // Leak data only if that won't generate an ASAN/valgrind warning
  if (!kMustFreeHeapAllocations) {
    shards_ = nullptr;
    num_shards_ = 0;
  }
}

size_t DLRUCache::TEST_GetDLRUSize() {
  size_t lm_lru_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    lm_lru_size_of_all_shards += shards_[i].TEST_GetDLRUSize();
  }
  return lm_lru_size_of_all_shards;
}

double DLRUCache::GetHighPriPoolRatio() {
  double result = 0.0;
  if (num_shards_ > 0) {
    result = shards_[0].GetHighPriPoolRatio();
  }
  return result;
}

void DLRUCache::WaitAll(std::vector<Handle*>& handles) {
  // TODO: no implemented
}

std::shared_ptr<Cache> NewDLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio, double rm_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy) {
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
  if (rm_ratio < 0.0 || rm_ratio > 1.0) {
    // invalid rm_ratio
    return nullptr;
  }
  return std::make_shared<DLRUCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      rm_ratio, std::move(memory_allocator), use_adaptive_mutex,
      metadata_charge_policy);
}

std::shared_ptr<Cache> NewDLRUCache(const DLRUCacheOptions& cache_opts) {
  return NewDLRUCache(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.high_pri_pool_ratio,
      cache_opts.rm_ratio, cache_opts.memory_allocator,
      cache_opts.use_adaptive_mutex, cache_opts.metadata_charge_policy);
}
}  // namespace ROCKSDB_NAMESPACE
