SET(SRC_LIST database_connection.cc connector.cc mysql_operation.cc)

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
    ../libs/log4cplus/include/
    ../libs/zlib/include64
    ../libs/yaml-cpp/include/
    ../libs/mysql/include/)

LINK_DIRECTORIES (
    ${PROJECT_SOURCE_DIR}/build/src/libs/common
    ${PROJECT_SOURCE_DIR}/build/src/core
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib/libz64.a
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/build/src/daemons
    ${PROJECT_SOURCE_DIR}/build/src/libs/stat
    ${PROJECT_SOURCE_DIR}/src/libs/mysql/libs
)

LINK_LIBRARIES(liblog4cplus.a) 
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

ADD_DEFINITIONS ("-g -fPIC -fpermissive -std=gnu++11 -DOMN_PLATFORM_UNIX -Wl,--no-undefined -Xlinker -zmuldefs")

ADD_EXECUTABLE (connector ${SRC_LIST})

TARGET_LINK_LIBRARIES(connector liblog4cplus.a libyaml-cpp.a  libz64.a libdtcd.a libcommon.a libdaemons.a libstat.a)