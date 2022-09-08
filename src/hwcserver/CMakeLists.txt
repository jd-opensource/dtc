FILE(GLOB_RECURSE SRC_LIST ./*.cc  ../connector/*tion.cc)
message(${SRC_LIST})

include(../utils.cmake)

INCLUDE_DIRECTORIES (
    ./
    ../core
    ../daemons
    ../connector/
    ../libs/api/cc_api/include/
    ../libs/common/
    ../libs/stat/
    ../libs/log4cplus/include/
    ../libs/zlib/include64
    ../libs/yaml-cpp/include/
    ../libs/mysql/include/
    ../libs/hsql/include/
)

LINK_DIRECTORIES(
    .
    ${PROJECT_SOURCE_DIR}/src/libs/api/cc_api/libs/
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
LINK_LIBRARIES(libcore.a)
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libdaemons.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(libmysqlclient.a)
LINK_LIBRARIES(libdtcapi.so)

ADD_DEFINITIONS ("-g -fPIC -fpermissive -std=gnu++11 -DOMN_PLATFORM_UNIX -Wl,--no-undefined -Xlinker -zmuldefs")
ADD_DEFINITIONS(-Wno-builtin-macro-redefined)

ADD_EXECUTABLE(hwcserver  ${SRC_LIST})

TARGET_LINK_LIBRARIES(hwcserver  libcore.a libdaemons.a libstat.a libcommon.a liblog4cplus.a  libyaml-cpp.a  libz64.a libmysqlclient.a libdtcapi.so)
redefine_file_macro(hwcserver)