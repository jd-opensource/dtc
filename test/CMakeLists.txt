INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src/devel/cpp/)

LINK_DIRECTORIES(${PROJECT_SOURCE_DIR}/build/src/devel/cpp)

LINK_LIBRARIES(libdtc.2.so) 

ADD_EXECUTABLE(insert "insert.cc")
ADD_EXECUTABLE(get "get.cc")

TARGET_LINK_LIBRARIES(insert libdtc.2.so)
TARGET_LINK_LIBRARIES(get libdtc.2.so)



