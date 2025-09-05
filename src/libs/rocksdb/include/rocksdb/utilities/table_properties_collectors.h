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
#ifndef ROCKSDB_LITE
#include <atomic>
#include <memory>

#include "rocksdb/table_properties.h"

namespace rocksdb {

// A factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entires.
class CompactOnDeletionCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  virtual ~CompactOnDeletionCollectorFactory() {}

  virtual TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  // Change the value of sliding_window_size "N"
  // Setting it to 0 disables the delete triggered compaction
  void SetWindowSize(size_t sliding_window_size) {
    sliding_window_size_.store(sliding_window_size);
  }

  // Change the value of deletion_trigger "D"
  void SetDeletionTrigger(size_t deletion_trigger) {
    deletion_trigger_.store(deletion_trigger);
  }

  virtual const char* Name() const override {
    return "CompactOnDeletionCollector";
  }

 private:
  friend std::shared_ptr<CompactOnDeletionCollectorFactory>
  NewCompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                       size_t deletion_trigger);
  // A factory of a table property collector that marks a SST
  // file as need-compaction when it observe at least "D" deletion
  // entries in any "N" consecutive entires.
  //
  // @param sliding_window_size "N"
  // @param deletion_trigger "D"
  CompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                    size_t deletion_trigger)
      : sliding_window_size_(sliding_window_size),
        deletion_trigger_(deletion_trigger) {}

  std::atomic<size_t> sliding_window_size_;
  std::atomic<size_t> deletion_trigger_;
};

// Creates a factory of a table property collector that marks a SST
// file as need-compaction when it observe at least "D" deletion
// entries in any "N" consecutive entires.
//
// @param sliding_window_size "N". Note that this number will be
//     round up to the smallest multiple of 128 that is no less
//     than the specified size.
// @param deletion_trigger "D".  Note that even when "N" is changed,
//     the specified number for "D" will not be changed.
extern std::shared_ptr<CompactOnDeletionCollectorFactory>
NewCompactOnDeletionCollectorFactory(size_t sliding_window_size,
                                     size_t deletion_trigger);
}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
