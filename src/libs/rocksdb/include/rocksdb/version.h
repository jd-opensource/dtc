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

#define ROCKSDB_MAJOR 6
#define ROCKSDB_MINOR 6
#define ROCKSDB_PATCH 0

// Do not use these. We made the mistake of declaring macros starting with
// double underscore. Now we have to live with our choice. We'll deprecate these
// at some point
#define __ROCKSDB_MAJOR__ ROCKSDB_MAJOR
#define __ROCKSDB_MINOR__ ROCKSDB_MINOR
#define __ROCKSDB_PATCH__ ROCKSDB_PATCH
