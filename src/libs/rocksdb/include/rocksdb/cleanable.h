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

namespace rocksdb {

class Cleanable {
 public:
  Cleanable();
  // No copy constructor and copy assignment allowed.
  Cleanable(Cleanable&) = delete;
  Cleanable& operator=(Cleanable&) = delete;

  ~Cleanable();

  // Move constructor and move assignment is allowed.
  Cleanable(Cleanable&&);
  Cleanable& operator=(Cleanable&&);

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.
  //
  // Note that unlike all of the preceding methods, this method is
  // not abstract and therefore clients should not override it.
  typedef void (*CleanupFunction)(void* arg1, void* arg2);
  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);
  void DelegateCleanupsTo(Cleanable* other);
  // DoCleanup and also resets the pointers for reuse
  inline void Reset() {
    DoCleanup();
    cleanup_.function = nullptr;
    cleanup_.next = nullptr;
  }

 protected:
  struct Cleanup {
    CleanupFunction function;
    void* arg1;
    void* arg2;
    Cleanup* next;
  };
  Cleanup cleanup_;
  // It also becomes the owner of c
  void RegisterCleanup(Cleanup* c);

 private:
  // Performs all the cleanups. It does not reset the pointers. Making it
  // private
  // to prevent misuse
  inline void DoCleanup() {
    if (cleanup_.function != nullptr) {
      (*cleanup_.function)(cleanup_.arg1, cleanup_.arg2);
      for (Cleanup* c = cleanup_.next; c != nullptr;) {
        (*c->function)(c->arg1, c->arg2);
        Cleanup* next = c->next;
        delete c;
        c = next;
      }
    }
  }
};

}  // namespace rocksdb
