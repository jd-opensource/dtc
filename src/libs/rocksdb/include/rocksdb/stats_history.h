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

#include <map>
#include <string>

#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace rocksdb {

class DBImpl;

// StatsHistoryIterator is the main interface for users to programmatically
// access statistics snapshots that was automatically stored by RocksDB.
// Depending on options, the stats can be in memory or on disk.
// The stats snapshots are indexed by time that they were recorded, and each
// stats snapshot contains individual stat name and value at the time of
// recording.
// Example:
//   std::unique_ptr<StatsHistoryIterator> stats_iter;
//   Status s = db->GetStatsHistory(0 /* start_time */,
//                                  env->NowMicros() /* end_time*/,
//                                  &stats_iter);
//   if (s.ok) {
//     for (; stats_iter->Valid(); stats_iter->Next()) {
//       uint64_t stats_time = stats_iter->GetStatsTime();
//       const std::map<std::string, uint64_t>& stats_map =
//           stats_iter->GetStatsMap();
//       process(stats_time, stats_map);
//     }
//   }
class StatsHistoryIterator {
 public:
  StatsHistoryIterator() {}
  virtual ~StatsHistoryIterator() {}

  virtual bool Valid() const = 0;

  // Moves to the next stats history record.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Return the time stamp (in seconds) when stats history is recorded.
  // REQUIRES: Valid()
  virtual uint64_t GetStatsTime() const = 0;

  virtual int GetFormatVersion() const { return -1; }

  // Return the current stats history as an std::map which specifies the
  // mapping from stats name to stats value . The underlying storage
  // for the returned map is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual const std::map<std::string, uint64_t>& GetStatsMap() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const = 0;
};

}  // namespace rocksdb
