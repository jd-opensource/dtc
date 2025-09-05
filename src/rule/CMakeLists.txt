FILE(GLOB_RECURSE SRC_LIST ./*.cc ./*.c)

include(../utils.cmake)

# Add header file search path, equivalent to gcc -I
INCLUDE_DIRECTORIES(
    .
    ../libs/yaml-cpp/include/
    ../libs/log4cplus/include/
    ../libs/hsql/include)

# Add .so/.a file search path, equivalent to gcc -L
LINK_DIRECTORIES(
    .
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/src/libs/hsql/libs
    ${PROJECT_SOURCE_DIR}/src/libs/mxml/libs)

# Add link libraries, equivalent to gcc -l
LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libmxml.a)
LINK_LIBRARIES(libsqlparser.so) 
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(dl)

# Compilation parameters
ADD_DEFINITIONS("-g -rdynamic -fPIC -D_CORE_ -fpermissive -std=gnu++11 -D_GLIBCXX_USE_CXX11_ABI=0 -export-dynamic -Wl,--version-script,core.export.lst")
ADD_DEFINITIONS(-Wno-builtin-macro-redefined)

# Compile to .so/.a
ADD_EXECUTABLE(rule.bin ${SRC_LIST})
ADD_LIBRARY(rule SHARED ${SRC_LIST})

# Link target files with library files
TARGET_LINK_LIBRARIES(rule libsqlparser.so liblog4cplus.a libyaml-cpp.a libmxml.a)
TARGET_LINK_LIBRARIES(rule.bin libsqlparser.so liblog4cplus.a libyaml-cpp.a libmxml.a)
redefine_file_macro(rule)
redefine_file_macro(rule.bin)