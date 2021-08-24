set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/daemons)
set (EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/daemons)

#添加头文件搜索路径，相当于gcc -I
include_directories (
                    ${PROJECT_SOURCE_DIR}/daemons
                    ${PROJECT_SOURCE_DIR}/libs/common
                    ${PROJECT_SOURCE_DIR}/libs/stat
                    ${PROJECT_SOURCE_DIR}/libs/log4cplus/include
                    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/include)
    
#添加.so/.a文件搜索路径，相当于gcc -L
link_directories (
    ${PROJECT_SOURCE_DIR}/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/libs/common
    ${PROJECT_SOURCE_DIR}/libs/stat)

#把当前目录下的源文件列表存放到变量SRC_LIST1里
aux_source_directory (${PROJECT_SOURCE_DIR}/daemons SRC_LIST1)
aux_source_directory (${PROJECT_SOURCE_DIR}/libs/stat/stat_alarm_reporter SRC_LIST2)

#链接目标库
link_libraries(liblog4cplus.a) 
link_libraries(libcommon.a)
link_libraries(libyaml-cpp.a)
linK_libraries(libstat.a)
link_libraries(pthread)
link_libraries(dl)

#编译参数
add_definitions("-g -fPIC -fpermissive -std=gnu++11")

#编译为.so/.a
add_executable(faultlogger ${SRC_LIST1} ${SRC_LIST2})
add_library(daemons ${SRC_LIST1} ${SRC_LIST2})

#把目标文件与库文件进行链接
target_link_libraries(faultlogger libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a)
target_link_libraries(daemons libstat.a liblog4cplus.a libyaml-cpp.a libcommon.a)
