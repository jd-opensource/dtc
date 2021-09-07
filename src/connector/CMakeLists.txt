SET(SRC_LIST ./database_connection.cc ./connector.cc ./mysql_operation.cc)

INCLUDE_DIRECTORIES (
    .
    ../daemons
    ../libs/stat
    ../libs/common
    ../libs/common/log
    ../libs/common/config
    ../libs/common/daemon
    ../libs/common/socket
    ../libs/common/listener
    ../libs/common/task
    ../core
    ../libs/rocksdb/include 
    ../libs/rocksdb/logging
    ../libs/log4cplus/include/
    ../libs/zlib/include64
    ../libs/yaml-cpp/include/)

LINK_DIRECTORIES (
    ${PROJECT_SOURCE_DIR}/src/libs/rocksdb/lib
    ${PROJECT_SOURCE_DIR}/src/libs/common
    ${PROJECT_SOURCE_DIR}/src/core
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib/libz64.a
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/src/daemons
    ${PROJECT_SOURCE_DIR}/src/libs/stat
)

LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libz64.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(nsl)
LINK_LIBRARIES(stdc++)
LINK_LIBRARIES(dl)
LINK_LIBRARIES(z)
LINK_LIBRARIES(rocksdb)
LINK_LIBRARIES(libdtcd.a)
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libdaemons.a)
LINK_LIBRARIES(libstat.a)

ADD_DEFINITIONS ("-g -fPIC -fpermissive -std=gnu++11 -DROCKSDB_COMPILER=1 -DOMN_PLATFORM_UNIX -Wl,--no-undefined -Xlinker -zmuldefs")

ADD_EXECUTABLE (connector ${SRC_LIST})

TARGET_LINK_LIBRARIES(connector liblog4cplus.a libyaml-cpp.a  libz64.a lbdtcd.a libcommon.a libdaemons.a libstat.a)