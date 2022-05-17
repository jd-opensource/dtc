cmake_minimum_required(VERSION 2.8.8)

if (POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif (POLICY CMP0048)

project(gtest_test)

if (CMAKE_VERSION VERSION_LESS "3.1")
  add_definitions(-std=c++11)
else()
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  if(NOT CYGWIN)
    set(CMAKE_CXX_EXTENSIONS OFF)
  endif()
endif()

AUX_SOURCE_DIRECTORY(. main)

LINK_DIRECTORIES( ${PROJECT_SOURCE_DIR}/../lib )
INCLUDE_DIRECTORIES(  ${PROJECT_SOURCE_DIR}/../include )
add_executable(gtest_test ${main})

#下面这条语句中，链接了gmock、gtest以及pthread
#pthread是必要的，因为前两者会用到
TARGET_LINK_LIBRARIES(gtest_test gmock gtest pthread)
install(TARGETS gtest_test RUNTIME DESTINATION bin)
