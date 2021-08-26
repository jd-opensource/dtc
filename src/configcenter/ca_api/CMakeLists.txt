SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/configcenter/ca_api)
#添加头文件搜索路径，相当于gcc -I
include_directories (.)
    
#把当前目录下的源文件列表存放到变量SRC_LIST1里
aux_source_directory (. SRC_LIST1)

#编译参数
add_definitions("-g -Wall -fPIC")

#编译为.so/.a
add_executable(client-tool ${SRC_LIST1})
add_library(app-client ${SRC_LIST1})


