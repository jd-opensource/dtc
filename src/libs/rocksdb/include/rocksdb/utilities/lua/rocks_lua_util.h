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
// lua headers
extern "C" {
#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>
}

#ifdef LUA
#include <string>
#include <vector>

#include "rocksdb/utilities/lua/rocks_lua_custom_library.h"

namespace rocksdb {
namespace lua {
class LuaStateWrapper {
 public:
  explicit LuaStateWrapper(const std::string& lua_script) {
    lua_state_ = luaL_newstate();
    do_init(lua_script, {});
  }
  LuaStateWrapper(
      const std::string& lua_script,
      const std::vector<std::shared_ptr<RocksLuaCustomLibrary>>& libraries) {
    lua_state_ = luaL_newstate();
    do_init(lua_script, libraries);
  }
  lua_State* GetLuaState() const { return lua_state_; }
  ~LuaStateWrapper() { lua_close(lua_state_); }

 private:
  void do_init(
      const std::string& lua_script,
      const std::vector<std::shared_ptr<RocksLuaCustomLibrary>>& libraries) {
    if (lua_state_) {
      luaL_openlibs(lua_state_);
      for (const auto& library : libraries) {
        luaL_openlib(lua_state_, library->Name(), library->Lib(), 0);
        library->CustomSetup(lua_state_);
      }
      luaL_dostring(lua_state_, lua_script.c_str());
    }
  }

  lua_State* lua_state_;
};
}  // namespace lua
}  // namespace rocksdb
#endif  // LUA
