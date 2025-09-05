ADD_SUBDIRECTORY (./lib)	

FILE(GLOB_RECURSE SRC_LIST ./*.cc ./*.c)

include(../utils.cmake)

#add header file search path, equivalent to gcc -I
INCLUDE_DIRECTORIES(
    . ./algorithm ./buffer ./chain
    ./data ./hotbk ./log ./mem
    ./misc ./node ./nodegroup ./raw
    ./task ./time ./tree  
    ../libs/common
    ../devel/cpp
    ../daemons
    ../libs/stat
    ../libs/log4cplus/include
    ../libs/zlib/include64
    ../libs/yaml-cpp/include
    ../libs/hsql/include
    ../libs/mysql/include/)

#add .so/.a file search path, equivalent to gcc -L
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/libs/mysql/libs
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib/
    ${PROJECT_SOURCE_DIR}/src/libs/hsql/libs
    ${PROJECT_SOURCE_DIR}/build/src/libs/common
    ${PROJECT_SOURCE_DIR}/build/src/libs/stat
    ${PROJECT_SOURCE_DIR}/build/src/daemons)

#add link libraries, equivalent to gcc -l
LINK_LIBRARIES(libmysqlclient.a)
LINK_LIBRARIES(liblog4cplus.a)
LINK_LIBRARIES(libsqlparser.a) 
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(libdaemons.a)
LINK_LIBRARIES(libz64.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(dl)

#compilation parameters
ADD_DEFINITIONS("-g -rdynamic -fPIC -D_CORE_ -fpermissive -std=gnu++11 -D_GLIBCXX_USE_CXX11_ABI=0 -export-dynamic -Wl,--version-script,core.export.lst")
ADD_DEFINITIONS(-Wno-builtin-macro-redefined)

#compile to .so/.a
ADD_EXECUTABLE(dtcd ${SRC_LIST})

#link target files with library files
TARGET_LINK_LIBRARIES(dtcd libdaemons.a libstat.a libsqlparser.a libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a libmysqlclient.a)
redefine_file_macro(dtcd)