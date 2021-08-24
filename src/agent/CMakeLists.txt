cmake_minimum_required (VERSION 3.5)
project (agent)

#添加头文件搜索路径，相当于gcc -I
include_directories(. ./common ./event 
                    ./hashkit ./proto)


#把当前目录下的源文件列表存放到变量SRC_LIST1里
aux_source_directory(. SRC_LIST1)
aux_source_directory(./common SRC_LIST2)
aux_source_directory(./event SRC_LIST3)
aux_source_directory(./hashkit SRC_LIST4)
aux_source_directory(./proto STRC_LIST5)

#链接目标库
link_libraries(rt) 
link_libraries(mxml)
link_libraries(pthread)
#编译参数
add_definitions("-pg -DCONFIG_REGPARM=3 -DHAVE_STATS=1 -DUSE_COMPATIBLE_MODE=1 -DHAVE_ASSERT_LOG=1")

#编译为.so/.a
add_executable(dtcagent ${SRC_LIST1} ${SRC_LIST2} ${SRC_LIST3} ${SRC_LIST4} ${SRC_LIST5})

