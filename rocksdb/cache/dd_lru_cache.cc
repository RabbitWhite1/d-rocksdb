//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/dd_lru_cache.h"

#include <cassert>
#include <cstdint>
#include <cstdio>

#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "port/lang.h"
#include "table/block_based/block.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

DDLRUHandleTable::DDLRUHandleTable(int max_upper_hash_bits)
    : length_bits_(/* historical starting size*/ 4),
      list_(new DDLRUHandle* [size_t{1} << length_bits_] {}),
      elems_(0),
      max_length_bits_(max_upper_hash_bits) {}

DDLRUHandleTable::~DDLRUHandleTable() {
  ApplyToEntriesRange(
      [](DDLRUHandle* h) {
        if (!h->HasRefs()) {
          h->Free();
        }
      },
      0, uint32_t{1} << length_bits_);
}

DDLRUHandle* DDLRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

DDLRUHandle* DDLRUHandleTable::Insert(DDLRUHandle* h) {
  DDLRUHandle** ptr = FindPointer(h->key(), h->hash);
  DDLRUHandle* old = *ptr;
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

DDLRUHandle* DDLRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  DDLRUHandle** ptr = FindPointer(key, hash);
  DDLRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

DDLRUHandle** DDLRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  DDLRUHandle** ptr = &list_[hash >> (32 - length_bits_)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void DDLRUHandleTable::Resize() {
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
  std::unique_ptr<DDLRUHandle* []> new_list {
    new DDLRUHandle* [size_t{1} << new_length_bits] {}
  };
  uint32_t count = 0;
  for (uint32_t i = 0; i < old_length; i++) {
    DDLRUHandle* h = list_[i];
    while (h != nullptr) {
      DDLRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      DDLRUHandle** ptr = &new_list[hash >> (32 - new_length_bits)];
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

DDLRUCacheShard::DDLRUCacheShard(
    size_t capacity, bool strict_capacity_limit, double high_pri_pool_ratio,
    double rm_ratio, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy, int max_upper_hash_bits,
    const std::shared_ptr<RemoteMemory>&, const size_t shard_id,
    const std::shared_ptr<LocalMemory>& local_memory)
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
      shard_id_(shard_id),
      local_memory_(local_memory) {
  set_metadata_charge_policy(metadata_charge_policy);
  // Make empty circular linked list
  lm_lru_.next = &lm_lru_;
  lm_lru_.prev = &lm_lru_;
  lm_lru_low_pri_ = &lm_lru_;
  rm_lru_.next = &rm_lru_;
  rm_lru_.prev = &rm_lru_;
  SetCapacity(capacity);
  if (rm_ratio > 0.0) {
    std::string server_ip = "10.0.0.5";
    // remote_memory_ = std::make_shared<RemoteMemory>(
    //     new FFBasedRemoteMemoryAllocator(), server_ip, capacity * rm_ratio);
    remote_memory_ = std::make_shared<RemoteMemory>(
        new BlockBasedRemoteMemoryAllocator(4096ul), server_ip,
        capacity * rm_ratio, shard_id_);
    remote_memory_->init_lm(local_memory_);
  } else {
    remote_memory_ = nullptr;
  }
  last_evicted_handle = nullptr;
}

void DDLRUCacheShard::EraseUnRefEntries() {
  assert(false && "Not Debugged.");
  autovector<DDLRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    while (lm_lru_.next != &lm_lru_) {
      DDLRUHandle* old = lm_lru_.next;
      // DDLRU list contains only elements which can be evicted
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

void DDLRUCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, void* value, size_t charge,
                             bool is_local, DeleterFn deleter)>& callback,
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
      [callback](DDLRUHandle* h) {
        DeleterFn deleter = h->IsSecondaryCacheCompatible()
                                ? h->info_.helper->del_cb
                                : h->info_.deleter;
        callback(h->key(), h->value, h->charge, h->IsLocal(), deleter);
      },
      index_begin, index_end);
}

void DDLRUCacheShard::TEST_GetDDLRUList(DDLRUHandle** rm_lru,
                                        DDLRUHandle** lm_lru_low_pri) {
  MutexLock l(&mutex_);
  *rm_lru = &lm_lru_;
  *lm_lru_low_pri = lm_lru_low_pri_;
}

size_t DDLRUCacheShard::TEST_GetDDLRUSize() {
  MutexLock l(&mutex_);
  DDLRUHandle* lm_lru_handle = lm_lru_.next;
  size_t lm_lru_size = 0;
  while (lm_lru_handle != &lm_lru_) {
    lm_lru_size++;
    lm_lru_handle = lm_lru_handle->next;
  }
  return lm_lru_size;
}

double DDLRUCacheShard::GetHighPriPoolRatio() {
  MutexLock l(&mutex_);
  return high_pri_pool_ratio_;
}

void DDLRUCacheShard::LMLRU_Remove(DDLRUHandle* e) {
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
  // printf("removed lm DDLRUHandle(&=%p, charge=%lu, total_charge=%lu),
  // lm_usage_/lm_capacity_=%lu/%.0lf\n", e, e->charge, total_charge, lm_usage_,
  // lm_capacity_);
}

void DDLRUCacheShard::LMLRU_Insert(DDLRUHandle* e) {
  if (e && e->value) {
    assert(reinterpret_cast<Block*>(e->value)->size() < 5000);
  }
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of DDLRU list.
    e->next = &lm_lru_;
    e->prev = lm_lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    high_pri_pool_usage_ += total_charge;
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of DDLRU
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

void DDLRUCacheShard::RMLRU_Remove(DDLRUHandle* e) {
  if (e == &rm_lru_) {
    rm_lru_ = *e->next;
  } else {
    if (e->next != nullptr) {
      e->next->prev = e->prev;
    }
    if (e->prev != nullptr) {
      e->prev->next = e->next;
    }
  }
  e->prev = e->next = nullptr;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
  assert(rm_lru_usage_ >= total_charge);
  rm_lru_usage_ -= total_charge;
}

void DDLRUCacheShard::RMLRU_Insert(DDLRUHandle* e) {
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

void DDLRUCacheShard::MaintainPoolSize() {
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

void DDLRUCacheShard::EvictFromLRU(size_t charge,
                                   autovector<DDLRUHandle*>* deleted) {
  // this is for NON-RM
  while ((usage_ + charge) > capacity_ && lm_lru_.next != &lm_lru_) {
    DDLRUHandle* old = lm_lru_.next;
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

void DDLRUCacheShard::EvictFromLMLRU(
    size_t charge, autovector<DDLRUHandle*>* evicted_from_lm_list) {
  // assert: will later put into RM
  while ((lm_usage_ + charge) > lm_capacity_ && lm_lru_.next != &lm_lru_) {
    DDLRUHandle* old = lm_lru_.next;
    // DDLRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    assert(old->value != nullptr);
    LMLRU_Remove(old);
    // evicting from local to remote, so usage didn't change,
    // only when evicting from remote, total usage changes (see EvictFromRMLRU)
    old->next = old->prev = nullptr;
    // NOTE: we actually only evict value to remote, so use `slice_size` instead
    // of `charge`
    size_t old_slice_size = reinterpret_cast<Block*>(old->value)->size();
    assert(usage_ >= old_slice_size);
    assert(lm_usage_ >= old_slice_size);
    usage_ -= old_slice_size;
    lm_usage_ -= old_slice_size;
    evicted_from_lm_list->push_back(old);
  }
}

RMRegion* DDLRUCacheShard::EvictFromRMLRUAndFreeHandle(size_t charge) {
  size_t num_evicted = 0;
  do {
    // try to allocate and see if rm has enough space
    {
      RMRegion* rm_region = remote_memory_->rmalloc(charge);
      if (rm_region != nullptr) {
        usage_ += charge;
        rm_usage_ += charge;
        return rm_region;
      }
    }
    DDLRUHandle* old = rm_lru_.next;
    // DDLRU list contains only elements which can be evicted
    assert(old->InCache() && !old->HasRefs());
    RMLRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    size_t old_total_charge = old->CalcTotalCharge(metadata_charge_policy_);
    size_t old_slice_size = old->slice_size;
    // only when evicting from remote, total usage changes
    assert(usage_ >= old_total_charge);
    usage_ -= old_total_charge;
    assert(rm_usage_ >= old_slice_size);
    rm_usage_ -= old_slice_size;
    assert(lm_usage_ >= (old_total_charge - old_slice_size));
    lm_usage_ -= (old_total_charge - old_slice_size);
    assert(old->IsLocal() == false);
    remote_memory_->rmfree((RMRegion*)old->value);
    old->value = nullptr;
    old->Free();
    ++num_evicted;
  } while (rm_lru_.next != rm_lru_.prev);
  throw std::runtime_error("unable to evict from rm_lru and free handle");
}

void DDLRUCacheShard::DirectMoveValueToRM(
    DDLRUHandle* e, RMAsyncDirectRequest* rm_async_direct_request,
    RMRegion* rm_region) {
  // assert(e->InCache()); // not really. if again evicted from remote memory.
  assert(e->IsLocal());
  assert(!e->IsMovingToRM());
  assert(!e->IsFetchingFromRM());
  assert(remote_memory_);
  assert(e->info_.helper && "if using rm, should use helper with size_cb");
  std::chrono::high_resolution_clock::time_point begin, end;

  e->slice_size = reinterpret_cast<Block*>(e->value)->size();
  if (rm_region == nullptr) {
    rm_region = remote_memory_->rmalloc(e->slice_size);
    usage_ += e->slice_size;
    rm_usage_ += e->slice_size;
  } else {
    if (rm_region->size < e->slice_size) {
      throw std::runtime_error("rm region not enough space");
    }
  }
  uint64_t rm_addr = rm_region->addr;
  if (rm_addr <= 0) {
    throw std::runtime_error("should be able to allocate enough memory");
  }

  const char* lm_buf = reinterpret_cast<Block*>(e->value)->data();
  int ret = remote_memory_->direct_write(rm_addr, (void*)lm_buf, e->slice_size,
                                         rm_async_direct_request);
  if (ret != 0) {
    throw std::runtime_error("write failed");
  }
  e->SetMovingToRM(true);
  rm_async_direct_request->lm_value = e->value;
  rm_async_direct_request->size = e->slice_size;
  rm_async_direct_request->rm_region = rm_region;
  e->value = (void*)rm_async_direct_request;
}

void DDLRUCacheShard::DirectFetchValueFromRM(
    DDLRUHandle* e, RMAsyncDirectRequest* rm_async_direct_request) {
  std::chrono::high_resolution_clock::time_point begin, end;
  assert(e->InCache());
  assert(!e->IsLocal());
  assert(!e->IsMovingToRM());
  assert(!e->IsFetchingFromRM());
  assert(remote_memory_);
  RMRegion* rm_region = (RMRegion*)e->value;
  uint64_t rm_addr = rm_region->addr;

  MemoryRegion* lm_region =
      (MemoryRegion*)local_memory_->get_allocator()->Allocate(e->slice_size);
  rm_async_direct_request->read_buf = CacheAllocationPtr(
      reinterpret_cast<char*>(lm_region->addr),
      CustomDeleter(local_memory_->get_allocator(), lm_region));

  int ret = remote_memory_->direct_read(rm_addr, lm_region->addr, e->slice_size,
                                        rm_async_direct_request);
  if (ret != 0) {
    throw std::runtime_error("read failed");
  }

  e->SetFetchingFromRM(true);
  rm_async_direct_request->lm_region = lm_region;
  rm_async_direct_request->rm_region = rm_region;
  e->value = (void*)rm_async_direct_request;
}

void DDLRUCacheShard::SetCapacity(size_t capacity) {
  autovector<DDLRUHandle*> last_reference_list;
  {
    MutexLock l(&mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    lm_capacity_ = capacity_ * (1 - rm_ratio_);
    rm_capacity_ = capacity_ * rm_ratio_;
    // TODO: not sure whether this process is correct.
    EvictFromLMLRU(0, &last_reference_list);
  }

  // Try to insert the evicted entries into tiered cache
  // Free the entries outside of mutex for performance reasons
  for (auto entry : last_reference_list) {
    // TODO: maybe evict to remote memory
    entry->Free();
  }
}

void DDLRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  MutexLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

Status DDLRUCacheShard::InsertItem(DDLRUHandle* e, Cache::Handle** handle,
                                   bool free_handle_on_fail) {
  Status s = Status::OK();
  autovector<DDLRUHandle*> evict_to_rm_list;
  autovector<DDLRUHandle*> last_reference_list;
  size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);

  DDLRUHandle* old;

  {
    MutexLock l(&mutex_);

    // Free the space following strict DDLRU policy until enough space
    // is freed or the rm_lru list is empty
    if (remote_memory_) {
      EvictFromLMLRU(total_charge, &evict_to_rm_list);

      for (auto lm_entry : evict_to_rm_list) {
        // 1. evict rm_entry from RM to free enough space for this lm_entry
        size_t lm_entry_slice_size =
            reinterpret_cast<Block*>(lm_entry->value)->size();
        if (last_evicted_handle) {
          WaitDirectMove(last_evicted_handle);
          last_evicted_handle = nullptr;
        }
        RMRegion* rm_region = EvictFromRMLRUAndFreeHandle(lm_entry_slice_size);
        // TODO: if rm is full, and cannot evict enough space for putting entry
        //     from lm, then we directly evict lm_entry
        // 2. insert lm_entry to RM
        RMAsyncDirectRequest* move_req = new RMAsyncDirectRequest();
        DirectMoveValueToRM(lm_entry, move_req, rm_region);
        // 3. mark last_evicted_handle
        last_evicted_handle = lm_entry;
        WaitDirectMove(lm_entry);       // sync
        last_evicted_handle = nullptr;  // sync
      }
    } else {
      EvictFromLRU(total_charge, &last_reference_list);
    }

    if ((lm_usage_ + total_charge) > lm_capacity_ &&
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
        s = Status::Incomplete("Insert failed due to DDLRU cache being full.");
      }
    } else {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      old = table_.Insert(e);
      lm_usage_ += total_charge;
      usage_ += total_charge;
      if (old != nullptr) {
        if (old == last_evicted_handle) {
          WaitDirectMove(last_evicted_handle);
          last_evicted_handle = nullptr;
        }
        s = Status::OkOverwritten();
        assert(old->InCache());
        old->SetInCache(false);
        if (!old->HasRefs()) {
          // old is on DDLRU because it's in cache and its reference count is 0
          if (remote_memory_) {
            if (old->IsLocal()) {
              LMLRU_Remove(old);
            } else {
              RMLRU_Remove(old);
            }
          } else {
            LMLRU_Remove(old);
          }
          size_t old_total_charge =
              old->CalcTotalCharge(metadata_charge_policy_);
          assert(usage_ >= old_total_charge);
          usage_ -= old_total_charge;
          if (remote_memory_) {
            if (old->IsLocal()) {
              assert(lm_usage_ >= old_total_charge);
              lm_usage_ -= old_total_charge;
              old->FreeValue();
              old->Free();
            } else {
              size_t old_slice_size = old->slice_size;
              assert(lm_usage_ >= (old_total_charge - old_slice_size));
              assert(rm_usage_ >= old_total_charge);
              lm_usage_ -= (old_total_charge - old_slice_size);
              rm_usage_ -= old_slice_size;
              remote_memory_->rmfree((RMRegion*)old->value);
              old->Free();
            }
          } else {
            last_reference_list.push_back(old);
          }
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

  if (!remote_memory_) {
    // Try to insert the evicted entries into the secondary cache
    // Free the entries here outside of mutex for performance reasons
    // For using rm, this is brought forward, see above.
    for (auto entry : last_reference_list) {
      entry->FreeValue();
      entry->Free();
    }
  }
  return s;
}

Cache::Handle* DDLRUCacheShard::Lookup(
    const Slice& key, uint32_t hash,
    const ShardedCache::CacheItemHelper* /*helper*/,
    const ShardedCache::CreateCallback& /*create_cb*/,
    const ShardedCache::CreateFromUniquePtrCallback& create_from_ptr_cb,
    Cache::Priority /*priority*/, bool /*wait*/, Statistics* /*stats*/,
    bool* from_rm) {
  DDLRUHandle* e = nullptr;

  // std::chrono::high_resolution_clock::time_point begin, rm_lru_remove_end,
  //     fetch_req_end, evict_from_lm_end, last_evict_end, evict_rm_end,
  //     move_req_end, insert_rm_lru_end, fetch_wait_end, final_wait_end;
  // begin = std::chrono::high_resolution_clock::now();
  {
    MutexLock l(&mutex_);
    e = table_.Lookup(key, hash);
    if (e != nullptr) {
      assert(e->InCache());
      if (e->IsLocal() && !e->HasRefs()) {
        // The entry is in LMLRU since it's in hash, in local and has no
        // references external
        LMLRU_Remove(e);
      }
      if (e->IsLocal() && !e->IsMovingToRM()) {
        if (!e->HasRefs()) {
        }
        *from_rm = false;
      } else {
        size_t requiring_lm_slice_size = e->slice_size;
        RMAsyncDirectRequest* direct_fetch_req = nullptr;

        if (e->IsLocal() && e->IsMovingToRM()) {
          assert(e == last_evicted_handle);
          WaitDirectMoveAndUndo(e);
          last_evicted_handle = nullptr;
          // we don't need extra space, because the entry moving already has its
          // space in lm; although we still need to do eviction to guarantee
          // local space doesn't overflow
          requiring_lm_slice_size = 0;
        } else {
          assert(!e->IsLocal() && !e->IsMovingToRM());
          // 1. remove from RMLRU and fetch value
          // The entry is in RMLRU since it's in hash and in remote memory
          RMLRU_Remove(e);

          // fetch it from remote memory
          assert(e != last_evicted_handle);
          direct_fetch_req = new RMAsyncDirectRequest();
          DirectFetchValueFromRM(e, direct_fetch_req);
          // WaitDirectFetch(e, create_from_ptr_cb);  // sync
          // direct_fetch_req = nullptr;              // sync
        }

        // 2. evict to free enough space for this entry
        autovector<DDLRUHandle*> evict_to_rm_list;
        EvictFromLMLRU(requiring_lm_slice_size, &evict_to_rm_list);

        size_t num_evcit_to_rm = evict_to_rm_list.size();
        size_t i = 0;
        for (i = 0; i < num_evcit_to_rm; i++) {
          auto lm_entry = evict_to_rm_list[i];
          // 2.1. evict rm_entry from RM to free enough space for this lm_entry
          size_t lm_entry_slice_size =
              reinterpret_cast<Block*>(lm_entry->value)->size();

          // wait previous request
          if (last_evicted_handle) {
            if (e == last_evicted_handle) {
              // it is being moving to rm, undo this.
              WaitDirectMoveAndUndo(e);
            } else {
              WaitDirectMove(last_evicted_handle);
            }
            last_evicted_handle = nullptr;
          }
          RMRegion* rm_region =
              EvictFromRMLRUAndFreeHandle(lm_entry_slice_size);

          // 2.2. insert lm_entry to RM
          RMAsyncDirectRequest* move_req = new RMAsyncDirectRequest();
          DirectMoveValueToRM(lm_entry, move_req, rm_region);
          last_evicted_handle = lm_entry;
          WaitDirectMove(lm_entry);       // sync
          move_req = nullptr;             // sync
          last_evicted_handle = nullptr;  // sync

          // 2.3. wait for the read request
          if (i == 0 and direct_fetch_req != nullptr) {
            WaitDirectFetch(e, create_from_ptr_cb);
          }
        }

        if (local_memory_) {
          if (last_evicted_handle) {
            if (last_evicted_handle == e) {
              throw std::runtime_error("last_evicted_handle == e");
            }
            WaitDirectMove(e);
            last_evicted_handle = nullptr;
          }
        }
        if (i == 0 and direct_fetch_req != nullptr) {
          WaitDirectFetch(e, create_from_ptr_cb);
        }

        *from_rm = true;
      }
      e->Ref();
      e->SetHit();
    }
  }
  if (e && e->value) {
    assert(reinterpret_cast<Block*>(e->value)->size() < 5000);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

bool DDLRUCacheShard::Ref(Cache::Handle* h) {
  DDLRUHandle* e = reinterpret_cast<DDLRUHandle*>(h);
  MutexLock l(&mutex_);
  // To create another reference - entry must be already externally referenced
  assert(e->HasRefs());
  e->Ref();
  return true;
}

void DDLRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  MutexLock l(&mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

bool DDLRUCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  if (handle == nullptr) {
    return false;
  }
  DDLRUHandle* e = reinterpret_cast<DDLRUHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
    if (last_evicted_handle) {
      WaitDirectMove(last_evicted_handle);
      last_evicted_handle = nullptr;
    }
    last_reference = e->Unref();
    if (last_reference && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (remote_memory_) {
        if (e->IsLocal() && (lm_usage_ > lm_capacity_ || force_erase)) {
          // The DDLRU list must be empty since the cache is full
          assert(lm_lru_.next == &lm_lru_ || force_erase);
          // Take this opportunity and remove the item
          table_.Remove(e->key(), e->hash);
          e->SetInCache(false);
        } else if (!e->IsLocal() && (rm_usage_ > rm_capacity_ || force_erase)) {
          // The DDLRU list must be empty since the cache is full
          assert(rm_lru_.next == nullptr || force_erase);
          // Take this opportunity and remove the item
          table_.Remove(e->key(), e->hash);
          e->SetInCache(false);
        } else {
          // Put the item back on the DDLRU list, and don't free it
          LMLRU_Insert(e);
          last_reference = false;
        }
      } else if (!remote_memory_) {
        if ((usage_ > capacity_ || force_erase)) {
          // The DDLRU list must be empty since the cache is full
          assert(e->IsLocal() == true);
          assert(lm_lru_.next == &lm_lru_ || force_erase);
          // Take this opportunity and remove the item
          table_.Remove(e->key(), e->hash);
          e->SetInCache(false);
        } else {
          // Put the item back on the DDLRU list, and don't free it
          LMLRU_Insert(e);
          last_reference = false;
        }
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
      if (e->IsLocal()) {
        assert(lm_usage_ >= total_charge);
        lm_usage_ -= total_charge;
      } else {
        size_t slice_size = e->slice_size;
        assert(slice_size > 0);
        assert(lm_usage_ >= (total_charge - slice_size));
        assert(rm_usage_ >= total_charge);
        lm_usage_ -= (total_charge - slice_size);
        rm_usage_ -= slice_size;
      }
    }
  }

  // Free the entry here outside of mutex for performance reasons
  if (last_reference) {
    if (e->IsLocal()) {
      e->FreeValue();
    } else {
      remote_memory_->rmfree((RMRegion*)e->value);
    }
    e->Free();
  }
  return last_reference;
}

Status DDLRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                               size_t charge,
                               void (*deleter)(const Slice& key, void* value),
                               const Cache::CacheItemHelper* helper,
                               Cache::Handle** handle,
                               Cache::Priority priority) {
  // Allocate the memory here outside of the mutex
  // If the cache is full, we'll have to release it
  // It shouldn't happen very often though.
  DDLRUHandle* e = reinterpret_cast<DDLRUHandle*>(
      new char[sizeof(DDLRUHandle) - 1 + key.size()]);

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

  auto s = InsertItem(e, handle, /* free_handle_on_fail */ true);

  // printf("usage_=%lu/%lu, lm_usage=%lu/%lu, rm_usage=%lu/%lu\n", usage_,
  //        capacity_, lm_usage_, (uint64_t)lm_capacity_, rm_usage_,
  //        (uint64_t)rm_capacity_);

  return s;
}

void DDLRUCacheShard::Erase(const Slice& /*key*/, uint32_t /*hash*/) {
  throw std::runtime_error("just want to see when this is called");
}

// void DDLRUCacheShard::Erase(const Slice& key, uint32_t hash) {
//   throw std::runtime_error("just want to see when this is called");
//   DDLRUHandle* e;
//   bool last_reference = false;
//   {
//     MutexLock l(&mutex_);
//     e = table_.Remove(key, hash);
//     if (e != nullptr) {
//       assert(e->InCache());
//       e->SetInCache(false);
//       if (!e->HasRefs()) {
//         // The entry is in LRU since it's in hash and has no external
//         // references
//         if (e->IsLocal()) {
//           LMLRU_Remove(e);
//           size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
//           assert(usage_ >= total_charge);
//           usage_ -= total_charge;
//           last_reference = true;
//         }
//         else {
//           if (last_evicted_handle) {
//             Wait(last_evicted_handle);
//             last_evicted_handle = nullptr;
//           }
//           RMLRU_Remove(e);
//           size_t total_charge = e->CalcTotalCharge(metadata_charge_policy_);
//           size_t slice_size = e->slice_size;
//           assert(usage_ >= total_charge);
//           assert(lm_usage_ >= total_charge - slice_size);
//           assert(rm_usage_ >= slice_size);
//           usage_ -= total_charge;
//           lm_usage_ -= total_charge - slice_size;
//           rm_usage_ -= slice_size;
//           last_reference = true;
//         }
//       }
//     }
//   }

//   // Free the entry here outside of mutex for performance reasons
//   // last_reference will only be true if e != nullptr
//   if (last_reference) {
//     if (e->IsLocal()) {
//       e->FreeValue();
//     } else {
//       remote_memory_->rmfree((RMRegion*)e->value);
//     }
//     e->Free();
//   }
// }

bool DDLRUCacheShard::IsReady(Cache::Handle* /*handle*/) {
  // TODO: check whether need this
  assert(false && "Not Implemented");
  return false;
}

void DDLRUCacheShard::WaitDirectMove(DDLRUHandle* handle) {
  DDLRUHandle* e = handle;
  assert(e->IsMovingToRM());
  if (!e->IsMovingToRM()) {
    throw std::runtime_error("no need to moving");
  }
  RMAsyncDirectRequest* rm_async_direct_request =
      (RMAsyncDirectRequest*)e->value;

  rm_async_direct_request->wait();
  rm_async_direct_request->unlock_and_reset();

  // set back the value pointing to the data, so that it can be freed
  // local_memory_->get_allocator()->Deallocate(rm_async_direct_request->lm_region);
  e->value = rm_async_direct_request->lm_value;
  e->FreeValue();
  e->value = (void*)rm_async_direct_request->rm_region;

  assert(e->IsLocal() == true);
  e->SetLocal(false);
  e->SetMovingToRM(false);
  RMLRU_Insert(e);

  delete rm_async_direct_request;
}

void DDLRUCacheShard::WaitDirectMoveAndUndo(DDLRUHandle* handle) {
  DDLRUHandle* e = handle;
  assert(e->IsMovingToRM());
  if (!e->IsMovingToRM()) {
    throw std::runtime_error("no need to moving");
  }
  RMAsyncDirectRequest* rm_async_direct_request =
      (RMAsyncDirectRequest*)e->value;

  rm_async_direct_request->wait();
  rm_async_direct_request->unlock_and_reset();

  // set back the value pointing to the data
  e->value = rm_async_direct_request->lm_value;
  remote_memory_->rmfree(rm_async_direct_request->rm_region);
  rm_usage_ -= e->slice_size;
  lm_usage_ += e->slice_size;
  e->slice_size = 0;

  e->SetLocal(true);
  e->SetMovingToRM(false);
  size_t slice_size = reinterpret_cast<Block*>(e->value)->size();
  assert(slice_size < 5000);

  delete rm_async_direct_request;
}

void DDLRUCacheShard::WaitDirectFetch(
    DDLRUHandle* handle,
    ShardedCache::CreateFromUniquePtrCallback create_from_ptr_cb) {
  DDLRUHandle* e = handle;
  if (!e->IsFetchingFromRM()) {
    throw std::runtime_error("no need to wait fetching");
  }
  RMAsyncDirectRequest* rm_async_direct_request =
      (RMAsyncDirectRequest*)e->value;

  rm_async_direct_request->wait();
  rm_async_direct_request->unlock_and_reset();

  size_t ret_charge = 0;
  Status s = create_from_ptr_cb(std::move(rm_async_direct_request->read_buf),
                                e->slice_size, &e->value, &ret_charge);
  assert(s.ok());

  remote_memory_->rmfree(rm_async_direct_request->rm_region);
  rm_usage_ -= e->slice_size;
  lm_usage_ += e->slice_size;
  e->SetFetchingFromRM(false);
  e->slice_size = 0;
  e->SetLocal(true);
  size_t slice_size = reinterpret_cast<Block*>(e->value)->size();
  assert(slice_size < 5000);

  delete rm_async_direct_request;
}

size_t DDLRUCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t DDLRUCacheShard::GetLMUsage() const {
  MutexLock l(&mutex_);
  return lm_usage_;
}

size_t DDLRUCacheShard::GetRMUsage() const {
  MutexLock l(&mutex_);
  return rm_usage_;
}

size_t DDLRUCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  assert(usage_ >= lm_lru_usage_);
  return usage_ - lm_lru_usage_;
}

std::string DDLRUCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

DDLRUCache::DDLRUCache(size_t capacity, int num_shard_bits,
                       bool strict_capacity_limit, double high_pri_pool_ratio,
                       double rm_ratio,
                       std::shared_ptr<MemoryAllocator> allocator,
                       std::shared_ptr<MemoryAllocator> data_block_allocator,
                       bool use_adaptive_mutex,
                       CacheMetadataChargePolicy metadata_charge_policy)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator), std::move(data_block_allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<DDLRUCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(DDLRUCacheShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  printf(
      "DDLRUCache num_shards=%d, per_shard=%lu, rm_ratio=%lf, "
      "data_block_memory_allocator: %p\n",
      num_shards_, per_shard, rm_ratio, data_block_memory_allocator());
  if (data_block_memory_allocator()) {
    local_memory_ =
        std::make_shared<LocalMemory>(data_block_memory_allocator());
  }
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i])
        DDLRUCacheShard(per_shard, strict_capacity_limit, high_pri_pool_ratio,
                        rm_ratio, use_adaptive_mutex, metadata_charge_policy,
                        /* max_upper_hash_bits */ 32 - num_shard_bits, nullptr,
                        /*shard_id=*/i, local_memory_);
  }
}

DDLRUCache::~DDLRUCache() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~DDLRUCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* DDLRUCache::GetShard(uint32_t shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* DDLRUCache::GetShard(uint32_t shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* DDLRUCache::Value(Handle* handle) {
  return reinterpret_cast<const DDLRUHandle*>(handle)->value;
}

size_t DDLRUCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const DDLRUHandle*>(handle)->charge;
}

Cache::DeleterFn DDLRUCache::GetDeleter(Handle* handle) const {
  auto h = reinterpret_cast<const DDLRUHandle*>(handle);
  if (h->IsSecondaryCacheCompatible()) {
    return h->info_.helper->del_cb;
  } else {
    return h->info_.deleter;
  }
}

uint32_t DDLRUCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const DDLRUHandle*>(handle)->hash;
}

void DDLRUCache::DisownData() {
  // Leak data only if that won't generate an ASAN/valgrind warning
  if (!kMustFreeHeapAllocations) {
    shards_ = nullptr;
    num_shards_ = 0;
  }
}

size_t DDLRUCache::TEST_GetDDLRUSize() {
  size_t lm_lru_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    lm_lru_size_of_all_shards += shards_[i].TEST_GetDDLRUSize();
  }
  return lm_lru_size_of_all_shards;
}

double DDLRUCache::GetHighPriPoolRatio() {
  double result = 0.0;
  if (num_shards_ > 0) {
    result = shards_[0].GetHighPriPoolRatio();
  }
  return result;
}

void DDLRUCache::WaitAll(std::vector<Handle*>& /*handles*/) {
  // TODO: no implemented
}

std::shared_ptr<Cache> NewDDLRUCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio, double rm_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator,
    std::shared_ptr<MemoryAllocator> data_block_memory_allocator,
    bool use_adaptive_mutex, CacheMetadataChargePolicy metadata_charge_policy) {
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
  return std::make_shared<DDLRUCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      rm_ratio, memory_allocator, data_block_memory_allocator,
      use_adaptive_mutex, metadata_charge_policy);
}

std::shared_ptr<Cache> NewDDLRUCache(const DDLRUCacheOptions& cache_opts) {
  return NewDDLRUCache(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.high_pri_pool_ratio,
      cache_opts.rm_ratio, cache_opts.memory_allocator,
      cache_opts.data_block_memory_allocator, cache_opts.use_adaptive_mutex,
      cache_opts.metadata_charge_policy);
}
}  // namespace ROCKSDB_NAMESPACE
