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

#include "rocksdb/env.h"
#include "rocksdb/statistics.h"

namespace rocksdb {

class ConcurrentTaskLimiter {
 public:
  virtual ~ConcurrentTaskLimiter() {}

  // Returns a name that identifies this concurrent job limiter.
  virtual const std::string& GetName() const = 0;

  // Set max concurrent tasks.
  // limit = 0 means no new job allowed.
  // limit < 0 means no limitation.
  virtual void SetMaxOutstandingTask(int32_t limit) = 0;

  // Reset to unlimited max concurrent job.
  virtual void ResetMaxOutstandingTask() = 0;

  // Returns current outstanding job count.
  virtual int32_t GetOutstandingTask() const = 0;
};

// Create a ConcurrentTaskLimiter that can be shared with mulitple CFs
// across RocksDB instances to control concurrent tasks.
//
// @param name: Name of the limiter.
// @param limit: max concurrent tasks.
//        limit = 0 means no new job allowed.
//        limit < 0 means no limitation.
extern ConcurrentTaskLimiter* NewConcurrentTaskLimiter(const std::string& name,
                                                       int32_t limit);

}  // namespace rocksdb
