set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/core)
set (EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/core)

#把当前目录下的源文件列表存放到变量SRC_LIST里
FILE(GLOB_RECURSE SRC_LIST ./*.cc ./*.c)

#添加头文件搜索路径，相当于gcc -I
include_directories (
                    . ./algorithm  ./chain ./data
                    ./hotbk ./log ./mem ./misc ./node
                    ./nodegroup ./raw ./task ./time ./tree ./buffer
                    ../libs/common
                    ../devel/cpp
                    ../daemons
                    ../libs/stat
                    ../connector
                    ../libs/log4cplus/include
                    ../libs/zlib/include64
                    ../libs/yaml-cpp/include)
    
#添加.so/.a文件搜索路径，相当于gcc -L
link_directories (
    ${PROJECT_SOURCE_DIR}/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/libs/zlib/lib/
    ${PROJECT_SOURCE_DIR}/libs/common
    ${PROJECT_SOURCE_DIR}/libs/stat
    ${PROJECT_SOURCE_DIR}/daemons)

#添加链接库，相当于gcc -l
link_libraries(liblog4cplus.a) 
link_libraries(libcommon.a)
link_libraries(libyaml-cpp.a)
linK_libraries(libstat.a)
link_libraries(libdaemons.a)
link_libraries(libz64.a)
link_libraries(pthread)
link_libraries(dl)
#编译参数
add_definitions (" -g -rdynamic -fPIC -fpermissive -D_GLIBCXX_USE_CXX11_ABI=0 -export-dynamic -Wl,--version-script,dtcd.export.lst -std=gnu++11")

#编译为.so/.a
add_executable (dtcd    ${SRC_LIST1}  ${SRC_LIST2}  ./buffer/buffer_flush ./buffer/buffer_pond  ${SRC_LIST4}  ${SRC_LIST5}
                        ${SRC_LIST6}  ${SRC_LIST7}  ${SRC_LIST8}  ${SRC_LIST9}  ${SRC_LIST10}
                        ${SRC_LIST11} ${SRC_LIST12} ${SRC_LIST13} ${SRC_LIST14} ${SRC_LIST15}
                        )


add_library (dtcdd  ${SRC_LIST1}  ${SRC_LIST2}  ./buffer/buffer_flush ./buffer/buffer_pond  ${SRC_LIST4}  ${SRC_LIST5}
                    ${SRC_LIST6}  ${SRC_LIST7}  ${SRC_LIST8}  ${SRC_LIST9}  ${SRC_LIST10}
                    ${SRC_LIST11} ${SRC_LIST12} ${SRC_LIST13} ${SRC_LIST14} ${SRC_LIST15}
                    )

#将目标文件与库文件链接
target_link_libraries(dtcd libdaemons.a libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a)
target_link_libraries(dtcdd libdaemons.a libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a)


