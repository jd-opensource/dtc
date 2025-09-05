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
#ifdef LUA

// lua headers
extern "C" {
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>
}

namespace rocksdb {
namespace lua {
// A class that used to define custom C Library that is callable
// from Lua script
class RocksLuaCustomLibrary {
 public:
  virtual ~RocksLuaCustomLibrary() {}
  // The name of the C library.  This name will also be used as the table
  // (namespace) in Lua that contains the C library.
  virtual const char* Name() const = 0;

  // Returns a "static const struct luaL_Reg[]", which includes a list of
  // C functions.  Note that the last entry of this static array must be
  // {nullptr, nullptr} as required by Lua.
  //
  // More details about how to implement Lua C libraries can be found
  // in the official Lua document http://www.lua.org/pil/26.2.html
  virtual const struct luaL_Reg* Lib() const = 0;

  // A function that will be called right after the library has been created
  // and pushed on the top of the lua_State.  This custom setup function
  // allows developers to put additional table or constant values inside
  // the same table / namespace.
  virtual void CustomSetup(lua_State* /*L*/) const {}
};
}  // namespace lua
}  // namespace rocksdb
#endif  // LUA
