FILE(GLOB_RECURSE SRC_LIST ./*.cc ./*.c)

include(../utils.cmake)

#添加头文件搜索路径，相当于gcc -I
INCLUDE_DIRECTORIES(
    .
    ../libs/yaml-cpp/include/
    ../libs/log4cplus/include/
    ../libs/hsql/include
    ../libs/net
    ../libs/mysql/include/)

#添加.so/.a文件搜索路径，相当于gcc -L
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/src/libs/hsql/libs
    ${PROJECT_SOURCE_DIR}/build/src/libs/net
    ${PROJECT_SOURCE_DIR}/src/libs/mysql/libs)

#添加链接库，相当于gcc -l
LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libsqlparser.a) 
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libmysqlclient.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(dl)

#编译参数
ADD_DEFINITIONS("-g -rdynamic -fPIC -D_CORE_ -fpermissive -std=gnu++11 -D_GLIBCXX_USE_CXX11_ABI=0 -export-dynamic -Wl,--version-script,core.export.lst")
ADD_DEFINITIONS(-Wno-builtin-macro-redefined)

#编译为.so/.a
ADD_EXECUTABLE(async-conn ${SRC_LIST})

#将目标文件与库文件链接
TARGET_LINK_LIBRARIES(async-conn libsqlparser.a liblog4cplus.a libyaml-cpp.a libnet.a libmysqlclient.a)
redefine_file_macro(async-conn)
