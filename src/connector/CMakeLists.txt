#SET(SRC_LIST database_connection.cc connector.cc mysql_operation.cc)

FILE(GLOB SRC_LIST ./*.cc)

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
    ../core/buffer
    ../libs/rocksdb/include/
    ../core/mem
    ../libs/log4cplus/include/
    ../libs/zlib/include64
    ../libs/yaml-cpp/include/
    ../libs/mysql/include/
    ../libs/hsql/include)

LINK_DIRECTORIES (
    ${PROJECT_SOURCE_DIR}/src/libs/mysql/libs
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/hsql/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/build/src/libs/common
    ${PROJECT_SOURCE_DIR}/build/src/core/lib
    ${PROJECT_SOURCE_DIR}/build/src/daemons
    ${PROJECT_SOURCE_DIR}/build/src/libs/stat
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
LINK_LIBRARIES(libdtcd.a)
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libdaemons.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(libmysqlclient.a)

ADD_DEFINITIONS ("-g -fPIC -fpermissive -std=gnu++11 -DOMN_PLATFORM_UNIX -Wl,--no-undefined -Xlinker -zmuldefs")

ADD_EXECUTABLE (connector ${SRC_LIST})

TARGET_LINK_LIBRARIES(connector  libdtcd.a libdaemons.a libstat.a libcommon.a liblog4cplus.a  libyaml-cpp.a  libz64.a libmysqlclient.a)