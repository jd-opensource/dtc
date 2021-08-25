SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/libs/common)

if(CMAKE_COMPILER_IS_GNUCXX)
    add_compile_options(-std=gnu++11)
endif(CMAKE_COMPILER_IS_GNUCXX)

#把当前目录下的源文件列表存放到变量SRC_LIST里
FILE(GLOB_RECURSE SRC_LIST ./*.cc ./*.c)

#添加头文件搜索路径，相当于gcc -I
INCLUDE_DIRECTORIES (
    .
    ../../devel/cpp
    ../../daemons
    ../stat
    ../log4cplus/include/
    ../zlib/include64
    ../yaml-cpp/include/)

#添加.so/.a文件搜索路径，相当于gcc -L
LINK_DIRECTORIES (
    ${PROJECT_SOURCE_DIR}/libs/zlib/lib/libz64.a
    ${PROJECT_SOURCE_DIR}/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/libs
)

#添加链接库，相当于gcc -l
LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libz64.a)
LINK_LIBRARIES(pthread)

#编译参数
ADD_DEFINITIONS ("-g -fPIC -fpermissive")
#编译为.so/.a
ADD_LIBRARY (common ${SRC_LIST})

#将目标文件与库文件链接
TARGET_LINK_LIBRARIES(common liblog4cplus.a libyaml-cpp.a  libz64.a)