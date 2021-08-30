
#设置库文件输出路径
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/src/devel/cpp)

#把当前目录下的源文件列表存放到变量SRC_LIST1里
FILE(GLOB SRC_LIST1 ./*.c ./*.cc )

SET(${SRC_LIST2} ../../libs/common/value.c
                 ../../libs/common/poll/poller.cc
                 ../../libs/common/timer/timer_lsit.cc
                 ../../libs/common/table/table_def.cc
                 ../../libs/common/mem_check.cc
                 ../../libs/common/algorithm/md5.cc
                 ../../libs/common/section.cc
                 ../../libs/common/decode/decode.cc
                 ../../libs/common/encode.cc
                 ../../libs/common/field/field_api.cc
                 ../../libs/common/packet/packet_base.cc
                 ../../libs/common/packet/packet_client.cc
                 ../../libs/common/task/task_base.cc
                 ../../libs/common/task/task_const.cc
                 ../../libs/common/algorithm/compress.cc
                 ../../libs/common/buffer.cc
                 ../../libs/common/thread/thread.cc)

#添加头文件搜索路径，相当于gcc -I
INCLUDE_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/devel/cpp 
    ${PROJECT_SOURCE_DIR}/src/libs/common 
    ${PROJECT_SOURCE_DIR}/src/configcenter/ca_api
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/include/
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/include64
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/include/)

#添加.so/.a文件搜索路径，相当于gcc -L
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib/
    ${PROJECT_SOURCE_DIR}/src/libs/common
    ${PROJECT_SOURCE_DIR}/src/configcenter/ca_api/)

#链接目标库
LINK_LIBRARIES(dl) 
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libz64.a)
LINK_LIBRARIES(libapp-client.a)
LINK_LIBRARIES(libcommon.a)

#编译参数
ADD_DEFINITIONS ("-g -rdynamic -fPIC -fpermissive -DCLIENTAPI -D_GLIBCXX_USE_CXX11_ABI=0 -std=gnu++11 ")

#编译为.so/.a
ADD_LIBRARY(dtc.2 SHARED ${SRC_LIST1} ${SRC_LIST2} )
ADD_LIBRARY(dtc STATIC ${SRC_LIST1} ${SRC_LIST2} )
ADD_LIBRARY(dtc.pic STATIC ${SRC_LIST1} ${SRC_LIST2})

#把目标文件与库文件进行链接
TARGET_LINK_LIBRARIES(dtc.2 libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a libapp-client.a)
TARGET_LINK_LIBRARIES(dtc libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a libapp-client.a)
TARGET_LINK_LIBRARIES(dtc.pic libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a libapp-client.a)

#设置make install 安装路径
INSTALL(TARGETS dtc.2 LIBRARY DESTINATION /usr/lib/)
