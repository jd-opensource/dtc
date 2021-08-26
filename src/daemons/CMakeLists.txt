SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/daemons)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/daemons)

if(CMAKE_COMPILER_IS_GNUCXX)
    add_compile_options(-std=gnu++11)
endif(CMAKE_COMPILER_IS_GNUCXX)

#添加头文件搜索路径，相当于gcc -I
INCLUDE_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/daemons
    ${PROJECT_SOURCE_DIR}/libs/common
    ${PROJECT_SOURCE_DIR}/libs/stat
    ${PROJECT_SOURCE_DIR}/libs/log4cplus/include
    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/include)
    
#添加.so/.a文件搜索路径，相当于gcc -L
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/libs/common
    ${PROJECT_SOURCE_DIR}/libs/stat)

#把当前目录下的源文件列表存放到变量SRC_LIST1里
FILE(GLOB_RECURSE SRC_LIST1 ./*.cc ./*.c)
#aux_source_directory(${PROJECT_SOURCE_DIR}/libs/stat/stat_alarm_reporter SRC_LIST2)

#链接目标库
LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(dl)

#编译参数
ADD_DEFINITIONS("-g -fPIC -fpermissive")

#编译为.so/.a
ADD_EXECUTABLE(faultlogger ${SRC_LIST1})
ADD_LIBRARY(daemons ${SRC_LIST1})

#把目标文件与库文件进行链接
TARGET_LINK_LIBRARIES(faultlogger libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a)
TARGET_LINK_LIBRARIES(daemons libstat.a liblog4cplus.a libyaml-cpp.a libcommon.a)
