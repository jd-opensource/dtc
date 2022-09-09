AUX_SOURCE_DIRECTORY(. SRC_LIST)
AUX_SOURCE_DIRECTORY(. main)
list(REMOVE_ITEM SRC_LIST "./cron_test.cc")

include(../utils.cmake)

INCLUDE_DIRECTORIES (
    .
    ../daemons
    ../libs/stat
    ../libs/common
    ../libs/common/config
    ../libs/common/daemon
    ../libs/common/socket
    ../libs/common/listener
    ../libs/common/task
    ../core
    ../core/buffer
    ../core/mem
    ../connector
    ../libs/rocksdb/include/
    ../libs/log4cplus/include/
    ../libs/zlib/include64
    ../libs/yaml-cpp/include/
    ../libs/mysql/include/
    ../libs/hsql/include
    ../libs/croncpp/include
    ../libs/mxml/include)

LINK_DIRECTORIES (
    ${PROJECT_SOURCE_DIR}/src/libs/mysql/libs
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/build/src/libs/common
    ${PROJECT_SOURCE_DIR}/build/src/core/lib
    ${PROJECT_SOURCE_DIR}/build/src/daemons
    ${PROJECT_SOURCE_DIR}/build/src/libs/stat
    ${PROJECT_SOURCE_DIR}/src/libs/hsql/libs
    ${PROJECT_SOURCE_DIR}/src/libs/mxml/libs
    ${PROJECT_SOURCE_DIR}/src/libs/google_test/lib
)

LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libsqlparser.a)
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libz64.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(nsl)
LINK_LIBRARIES(stdc++)
LINK_LIBRARIES(dl)
LINK_LIBRARIES(z)
LINK_LIBRARIES(libcore.a)
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libdaemons.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(libmysqlclient.a)

ADD_DEFINITIONS ("-g -fPIC -fpermissive -std=gnu++11 -DOMN_PLATFORM_UNIX -Wl,--no-undefined -Xlinker -zmuldefs")
ADD_DEFINITIONS(-Wno-builtin-macro-redefined)
ADD_EXECUTABLE (data-lifecycle-manager ${SRC_LIST} ../connector/database_connection.cc)
TARGET_LINK_LIBRARIES(data-lifecycle-manager libcore.a libdaemons.a libstat.a libcommon.a liblog4cplus.a libyaml-cpp.a libz64.a libsqlparser.a libmysqlclient.a libmxml.a)
redefine_file_macro(data-lifecycle-manager)

if(jdtestOpen)
    cmake_minimum_required(VERSION 2.8.8)

    if (POLICY CMP0048)
        cmake_policy(SET CMP0048 NEW)
    endif (POLICY CMP0048)
    
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
    list(REMOVE_ITEM main "./main.cc")
    list(REMOVE_ITEM main "./cron_test.cc")
    AUX_SOURCE_DIRECTORY(./unittest jdtestFiles)

    ADD_EXECUTABLE(gtest_data_lifecycle ${main} ${jdtestFiles} ../connector/database_connection.cc)
    target_include_directories(gtest_data_lifecycle  PUBLIC
    .
    ../daemons
    ../libs/stat
    ../libs/common
    ../libs/common/config
    ../libs/common/daemon
    ../libs/common/socket
    ../libs/common/listener
    ../libs/common/task
    ../core
    ../core/buffer
    ../core/mem
    ../connector
    ../libs/rocksdb/include/
    ../libs/log4cplus/include/
    ../libs/zlib/include64
    ../libs/yaml-cpp/include/
    ../libs/mysql/include/
    ../libs/hsql/include
    ../libs/croncpp/include
    ../libs/mxml/include
    ../libs/google_test/include
    )
    target_link_libraries(gtest_data_lifecycle core daemons stat common gtest_main gmock gtest dl pthread log4cplus sqlparser yaml-cpp z64 mysqlclient mxml)
    redefine_file_macro(gtest_data_lifecycle)
    SET_TARGET_PROPERTIES(gtest_data_lifecycle PROPERTIES RUNTIME_OUTPUT_DIRECTORY "./bin")
    install(TARGETS gtest_data_lifecycle RUNTIME DESTINATION bin)
endif()
