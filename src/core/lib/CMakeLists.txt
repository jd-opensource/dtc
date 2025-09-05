# # Set library file output path
# SET(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/src/core)
# # Set executable file output path
# SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/src/core)

FILE(GLOB_RECURSE SRC_LIST ../*.cc ../*.c)

# Add header file search path, equivalent to gcc -I
INCLUDE_DIRECTORIES(
    ../. ../algorithm ../buffer ../chain
    ../data ../hotbk ../log ../mem
    ../misc ../node ../nodegroup ../raw
    ../task ../time ../tree  
    ../../libs/common
    ../../devel/cpp
    ../../daemons
    ../../libs/stat
    ../../libs/log4cplus/include
    ../../libs/zlib/include64
    ../../libs/yaml-cpp/include)               

# Add .so/.a file search path, equivalent to gcc -L
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/src/libs/zlib/lib/
    ${PROJECT_SOURCE_DIR}/build/src/libs/common
    ${PROJECT_SOURCE_DIR}/build/src/libs/stat
    ${PROJECT_SOURCE_DIR}/build/src/daemons)

# Add link libraries, equivalent to gcc -l
LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(libdaemons.a)
LINK_LIBRARIES(libz64.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(dl)

# Compilation parameters
ADD_DEFINITIONS("-g -rdynamic -fPIC -fpermissive -std=gnu++11 -D_GLIBCXX_USE_CXX11_ABI=0 -export-dynamic -Wl,--version-script,core.export.lst")

# Compile to .so/.a
ADD_LIBRARY(core_static ${SRC_LIST})

# Link target files with library files
TARGET_LINK_LIBRARIES(core_static libdaemons.a libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a libz64.a)
SET_TARGET_PROPERTIES(core_static PROPERTIES OUTPUT_NAME "core")
SET_TARGET_PROPERTIES (core_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

