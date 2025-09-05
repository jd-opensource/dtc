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
#include <list>
#include <queue>
#include <string>

#include "filename.h"
#include "port.h"
// #include "util_logger.h"
#include "mutexlock.h"

namespace rocksdb {

#ifndef ROCKSDB_LITE
// Rolls the log file by size and/or time
class AutoRollLogger : public Logger {
 public:
  AutoRollLogger(Env* env, const std::string& dbname,
                 const std::string& db_log_dir, size_t log_max_size,
                 size_t log_file_time_to_roll, size_t keep_log_file_num,
                 const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL);

  using Logger::Logv;
  void Logv(const char* format, va_list ap) override;

  // Write a header entry to the log. All header information will be written
  // again every time the log rolls over.
  virtual void LogHeader(const char* format, va_list ap) override;

  // check if the logger has encountered any problem.
  Status get_status() {
    return status_;
  }

  size_t GetLogFileSize() const override {
    if (!logger_) {
      return 0;
    }

    std::shared_ptr<Logger> logger;
    {
      MutexLock l(&mutex_);
      // pin down the current logger_ instance before releasing the mutex.
      logger = logger_;
    }
    return logger->GetLogFileSize();
  }

  void Flush() override {
    std::shared_ptr<Logger> logger;
    {
      MutexLock l(&mutex_);
      // pin down the current logger_ instance before releasing the mutex.
      logger = logger_;
    }
    if (logger) {
      logger->Flush();
    }
  }

  virtual ~AutoRollLogger() {
    if (logger_ && !closed_) {
      logger_->Close();
    }
  }

  void SetCallNowMicrosEveryNRecords(uint64_t call_NowMicros_every_N_records) {
    call_NowMicros_every_N_records_ = call_NowMicros_every_N_records;
  }

  // Expose the log file path for testing purpose
  std::string TEST_log_fname() const {
    return log_fname_;
  }

  uint64_t TEST_ctime() const { return ctime_; }

 protected:
  // Implementation of close()
  virtual Status CloseImpl() override {
    if (logger_) {
      return logger_->Close();
    } else {
      return Status::OK();
    }
  }

 private:
  bool LogExpired();
  Status ResetLogger();
  void RollLogFile();
  // Read all names of old log files into old_log_files_
  // If there is any error, put the error code in status_
  void GetExistingFiles();
  // Delete old log files if it excceeds the limit.
  Status TrimOldLogFiles();
  // Log message to logger without rolling
  void LogInternal(const char* format, ...);
  // Serialize the va_list to a string
  std::string ValistToString(const char* format, va_list args) const;
  // Write the logs marked as headers to the new log file
  void WriteHeaderInfo();
  std::string log_fname_; // Current active info log's file name.
  std::string dbname_;
  std::string db_log_dir_;
  std::string db_absolute_path_;
  Env* env_;
  std::shared_ptr<Logger> logger_;
  // current status of the logger
  Status status_;
  const size_t kMaxLogFileSize;
  const size_t kLogFileTimeToRoll;
  const size_t kKeepLogFileNum;
  // header information
  std::list<std::string> headers_;
  // List of all existing info log files. Used for enforcing number of
  // info log files.
  // Full path is stored here. It consumes signifianctly more memory
  // than only storing file name. Can optimize if it causes a problem.
  std::queue<std::string> old_log_files_;
  // to avoid frequent env->NowMicros() calls, we cached the current time
  uint64_t cached_now;
  uint64_t ctime_;
  uint64_t cached_now_access_count;
  uint64_t call_NowMicros_every_N_records_;
  mutable port::Mutex mutex_;
};
#endif  // !ROCKSDB_LITE

// Facade to craete logger automatically
Status CreateLoggerFromOptions(const std::string& dbname,
                               const DBOptions& options,
                               std::shared_ptr<Logger>* logger);

}  // namespace rocksdb
