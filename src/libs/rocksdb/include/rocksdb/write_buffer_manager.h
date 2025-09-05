/*
* Copyright JD.com, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#pragma once

#include <atomic>
#include <cstddef>
#include "rocksdb/cache.h"

namespace rocksdb {

class WriteBufferManager {
 public:
  // _buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  // if `cache` is provided, we'll put dummy entries in the cache and cost
  // the memory allocated to the cache. It can be used even if _buffer_size = 0.
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {});
  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  ~WriteBufferManager();

  bool enabled() const { return buffer_size_ != 0; }

  bool cost_to_cache() const { return cache_rep_ != nullptr; }

  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }
  size_t buffer_size() const { return buffer_size_; }

  // Should only be called from write thread
  bool ShouldFlush() const {
    if (enabled()) {
      if (mutable_memtable_memory_usage() > mutable_limit_) {
        return true;
      }
      if (memory_usage() >= buffer_size_ &&
          mutable_memtable_memory_usage() >= buffer_size_ / 2) {
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        return true;
      }
    }
    return false;
  }

  void ReserveMem(size_t mem) {
    if (cache_rep_ != nullptr) {
      ReserveMemWithCache(mem);
    } else if (enabled()) {
      memory_used_.fetch_add(mem, std::memory_order_relaxed);
    }
    if (enabled()) {
      memory_active_.fetch_add(mem, std::memory_order_relaxed);
    }
  }
  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem) {
    if (enabled()) {
      memory_active_.fetch_sub(mem, std::memory_order_relaxed);
    }
  }
  void FreeMem(size_t mem) {
    if (cache_rep_ != nullptr) {
      FreeMemWithCache(mem);
    } else if (enabled()) {
      memory_used_.fetch_sub(mem, std::memory_order_relaxed);
    }
  }

 private:
  const size_t buffer_size_;
  const size_t mutable_limit_;
  std::atomic<size_t> memory_used_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
  struct CacheRep;
  std::unique_ptr<CacheRep> cache_rep_;

  void ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);
};
}  // namespace rocksdb
