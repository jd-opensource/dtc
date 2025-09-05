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

#include "rocksdb/db.h"
#include "rocksdb/types.h"

namespace rocksdb {

// Data associated with a particular version of a key. A database may internally
// store multiple versions of a same user key due to snapshots, compaction not
// happening yet, etc.
struct KeyVersion {
  KeyVersion() : user_key(""), value(""), sequence(0), type(0) {}

  KeyVersion(const std::string& _user_key, const std::string& _value,
             SequenceNumber _sequence, int _type)
      : user_key(_user_key), value(_value), sequence(_sequence), type(_type) {}

  std::string user_key;
  std::string value;
  SequenceNumber sequence;
  // TODO(ajkr): we should provide a helper function that converts the int to a
  // string describing the type for easier debugging.
  int type;
};

// Returns listing of all versions of keys in the provided user key range.
// The range is inclusive-inclusive, i.e., [`begin_key`, `end_key`], or
// `max_num_ikeys` has been reached. Since all those keys returned will be
// copied to memory, if the range covers too many keys, the memory usage
// may be huge. `max_num_ikeys` can be used to cap the memory usage.
// The result is inserted into the provided vector, `key_versions`.
Status GetAllKeyVersions(DB* db, Slice begin_key, Slice end_key,
                         size_t max_num_ikeys,
                         std::vector<KeyVersion>* key_versions);

Status GetAllKeyVersions(DB* db, ColumnFamilyHandle* cfh, Slice begin_key,
                         Slice end_key, size_t max_num_ikeys,
                         std::vector<KeyVersion>* key_versions);

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
