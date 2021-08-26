#设置库文件输出路径
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/connector)

IF(CMAKE_COMPILER_IS_GNUCXX)
    ADD_COMPILE_OPTIONS(-std=gnu++11)
ENDIF(CMAKE_COMPILER_IS_GNUCXX)

#把当前目录下的源文件列表存放到变量SRC_LIST里
FILE(GLOB_RECURSE SRC_LIST ./*.cc)

#添加头文件搜索路径，相当于gcc -I
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

#添加.so/.a文件搜索路径，相当于gcc -L
LINK_DIRECTORIES (
    ${PROJECT_SOURCE_DIR}/libs/rocksdb/lib
    ${PROJECT_SOURCE_DIR}/libs/common
    ${PROJECT_SOURCE_DIR}/core
    ${PROJECT_SOURCE_DIR}/libs/zlib/lib/libz64.a
    ${PROJECT_SOURCE_DIR}/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/daemons
    ${PROJECT_SOURCE_DIR}/libs/stat
)

#添加链接库，相当于gcc -l
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

#编译参数
ADD_DEFINITIONS ("-g -fPIC -fpermissive -DROCKSDB_COMPILER=1 -DOMN_PLATFORM_UNIX -Wl,--no-undefined -Xlinker -zmuldefs")
#编译为.so/.a
ADD_LIBRARY (rocksdb_connector ${SRC_LIST})

#将目标文件与库文件链接
TARGET_LINK_LIBRARIES(rocksdb_connector liblog4cplus.a libyaml-cpp.a  libz64.a lbdtcd.a libcommon.a libdaemons.a libstat.a)