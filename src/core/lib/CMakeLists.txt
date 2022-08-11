# #设置库文件输出路径
# SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/src/core)
# #设置可执行文件输出路径
# SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/src/core)

FILE(GLOB_RECURSE SRC_LIST ../*.cc ../*.c)

#添加头文件搜索路径，相当于gcc -I
INCLUDE_DIRECTORIES(
    ../. ../algorithm ../buffer ../chain
    ../data ../hotbk ../log ../mem
    ../misc ../node ../nodegroup ../raw
    ../task ../time ../tree  
    ../../libs/common
    ../../devel/cpp
    ../../daemons
    ../../libs/stat
    ../../libs/log4cplus/include
    ../../libs/zlib/include64
    ../../libs/yaml-cpp/include)               

#添加.so/.a文件搜索路径，相当于gcc -L
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib/
    ${PROJECT_SOURCE_DIR}/build/src/libs/common
    ${PROJECT_SOURCE_DIR}/build/src/libs/stat
    ${PROJECT_SOURCE_DIR}/build/src/daemons)

#添加链接库，相当于gcc -l
LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(libdaemons.a)
LINK_LIBRARIES(libz64.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(dl)

#编译参数
ADD_DEFINITIONS("-g -rdynamic -fPIC -fpermissive -std=gnu++11 -D_GLIBCXX_USE_CXX11_ABI=0 -export-dynamic -Wl,--version-script,core.export.lst")

#编译为.so/.a
ADD_LIBRARY(dtcd_static ${SRC_LIST})

#将目标文件与库文件链接
TARGET_LINK_LIBRARIES(dtcd_static libdaemons.a libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a)
SET_TARGET_PROPERTIES(dtcd_static PROPERTIES OUTPUT_NAME "core")
SET_TARGET_PROPERTIES (dtcd_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

