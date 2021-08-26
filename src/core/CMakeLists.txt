#设置库文件输出路径
SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/core)
#设置可执行文件输出路径
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/core)

if(CMAKE_COMPILER_IS_GNUCXX)
    add_compile_options(-std=gnu++11)
endif(CMAKE_COMPILER_IS_GNUCXX)


#把当前目录下的源文件列表存放到变量SRC_LIST里
FILE(GLOB_RECURSE SRC_LIST ./*.cc ./*.c)

#添加头文件搜索路径，相当于gcc -I
INCLUDE_DIRECTORIES(
    . ./algorithm ./buffer ./chain
    ./data ./hotbk ./log ./mem
    ./misc ./node ./nodegroup ./raw
    ./task ./time ./tree  
    ../libs/common
    ../devel/cpp
    ../daemons
    ../libs/stat
    ../connector
    ../libs/log4cplus/include
    ../libs/zlib/include64
    ../libs/yaml-cpp/include)               

#添加.so/.a文件搜索路径，相当于gcc -L
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/libs/zlib/lib/
    ${PROJECT_SOURCE_DIR}/libs/common
    ${PROJECT_SOURCE_DIR}/libs/stat
    ${PROJECT_SOURCE_DIR}/daemons)

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
ADD_DEFINITIONS(" -g -rdynamic -fPIC -fpermissive -D_GLIBCXX_USE_CXX11_ABI=0 -export-dynamic -Wl,--version-script,dtcd.export.lst")

#编译为.so/.a
ADD_EXECUTABLE(dtcd ${SRC_LIST})
ADD_LIBRARY(dtcd_static  ${SRC_LIST})

#将目标文件与库文件链接
TARGET_LINK_LIBRARIES(dtcd libdaemons.a libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a)
TARGET_LINK_LIBRARIES(dtcd_static libdaemons.a libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a)

SET_TARGET_PROPERTIES (dtcd_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES (dtcd PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(dtcd_static PROPERTIES OUTPUT_NAME "dtcd")

#设置make install 安装路径
INSTALL(TARGETS dtcd RUNTIME DESTINATION /usr/local/dtc/bin)

