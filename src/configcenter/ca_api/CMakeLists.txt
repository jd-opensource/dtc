SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/src/configcenter/ca_api)
#添加头文件搜索路径，相当于gcc -I
INCLUDE_DIRECTORIES(.)
    
#把当前目录下的源文件列表存放到变量SRC_LIST1里
AUX_SOURCE_DIRECTORY(. SRC_LIST1)

#编译参数
ADD_DEFINITIONS("-g -Wall -fPIC")

#编译为.so/.a
ADD_EXECUTABLE(client-tool ${SRC_LIST1})
ADD_LIBRARY(app-client ${SRC_LIST1})


