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

#include <stdint.h>
#include <string>

namespace rocksdb {

// How much perf stats to collect. Affects perf_context and iostats_context.
enum PerfLevel : unsigned char {
  kUninitialized = 0,             // unknown setting
  kDisable = 1,                   // disable perf stats
  kEnableCount = 2,               // enable only count stats
  kEnableTimeExceptForMutex = 3,  // Other than count stats, also enable time
                                  // stats except for mutexes
  // Other than time, also measure CPU time counters. Still don't measure
  // time (neither wall time nor CPU time) for mutexes.
  kEnableTimeAndCPUTimeExceptForMutex = 4,
  kEnableTime = 5,  // enable count and time stats
  kOutOfBounds = 6  // N.B. Must always be the last value!
};

// set the perf stats level for current thread
void SetPerfLevel(PerfLevel level);

// get current perf stats level for current thread
PerfLevel GetPerfLevel();

}  // namespace rocksdb
