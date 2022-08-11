INCLUDE_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/daemons
    ${PROJECT_SOURCE_DIR}/src/libs/common
    ${PROJECT_SOURCE_DIR}/src/libs/stat
    ${PROJECT_SOURCE_DIR}/src/core
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/include
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/include)
    
LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/libs/log4cplus/libs
    ${PROJECT_SOURCE_DIR}/src/libs/yaml-cpp/libs
    ${PROJECT_SOURCE_DIR}/build/src/libs/common
    ${PROJECT_SOURCE_DIR}/build/src/libs/stat)

include(../utils.cmake)

FILE(GLOB_RECURSE SRC_LIST1 ./*.cc ./*.c)


LINK_LIBRARIES(liblog4cplus.a) 
LINK_LIBRARIES(libcommon.a)
LINK_LIBRARIES(libyaml-cpp.a)
LINK_LIBRARIES(libstat.a)
LINK_LIBRARIES(pthread)
LINK_LIBRARIES(dl)

ADD_DEFINITIONS("-g -fPIC -fpermissive -std=gnu++11")
ADD_DEFINITIONS(-Wno-builtin-macro-redefined)

ADD_EXECUTABLE(dtc ${SRC_LIST1})

TARGET_LINK_LIBRARIES(dtc libstat.a libcommon.a libyaml-cpp.a liblog4cplus.a)
redefine_file_macro(dtc)