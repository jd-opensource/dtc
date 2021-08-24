set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/libs/stat)

#添加头文件搜索路径，相当于gcc -I
include_directories (
${PROJECT_SOURCE_DIR}/libs/stat
${PROJECT_SOURCE_DIR}/libs/common
${PROJECT_SOURCE_DIR}/libs/log4cplus/include
${PROJECT_SOURCE_DIR}/libs/yaml-cpp/include)
    
#添加.so/.a文件搜索路径，相当于gcc -L
link_directories (
${PROJECT_SOURCE_DIR}/libs/log4cplus/libs
${PROJECT_SOURCE_DIR}/libs/yaml-cpp/libs
${PROJECT_SOURCE_DIR}/libs/common)

#把当前目录下的源文件列表存放到变量SRC_LIST1里
aux_source_directory (. SRC_LIST1)

#链接目标库
link_libraries(liblog4cplus.a) 
link_libraries(libcommon.a)
link_libraries(libyaml-cpp.a)
link_libraries(pthread)
link_libraries(dl)

#编译参数
add_definitions("-g -fPIC -fpermissive -std=gnu++11")

#编译为.so/.a
add_executable(stattool ${SRC_LIST1})
add_library(stat ${SRC_LIST1})

#把目标文件与库文件进行链接
target_link_libraries(stattool libcommon.a libyaml-cpp.a liblog4cplus.a)
target_link_libraries(stat liblog4cplus.a libyaml-cpp.a libcommon.a)


