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

namespace rocksdb {

// Allow custom implementations of TraceWriter and TraceReader.
// By default, RocksDB provides a way to capture the traces to a file using the
// factory NewFileTraceWriter(). But users could also choose to export traces to
// any other system by providing custom implementations of TraceWriter and
// TraceReader.

// TraceWriter allows exporting RocksDB traces to any system, one operation at
// a time.
class TraceWriter {
 public:
  TraceWriter() {}
  virtual ~TraceWriter() {}

  virtual Status Write(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual uint64_t GetFileSize() = 0;
};

// TraceReader allows reading RocksDB traces from any system, one operation at
// a time. A RocksDB Replayer could depend on this to replay opertions.
class TraceReader {
 public:
  TraceReader() {}
  virtual ~TraceReader() {}

  virtual Status Read(std::string* data) = 0;
  virtual Status Close() = 0;
};

// Factory methods to read/write traces from/to a file.
Status NewFileTraceWriter(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceWriter>* trace_writer);
Status NewFileTraceReader(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceReader>* trace_reader);
}  // namespace rocksdb
