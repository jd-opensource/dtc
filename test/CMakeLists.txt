INCLUDE_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/src/devel/cpp/
    ${PROJECT_SOURCE_DIR}/src/libs/gtest/include
)

LINK_DIRECTORIES(
    ${PROJECT_SOURCE_DIR}/build/src/devel/cpp
    ${PROJECT_SOURCE_DIR}/src/libs/gtest/lib
)

LINK_LIBRARIES(libdtc.2.so) 
ADD_DEFINITIONS(" -std=gnu++11  -D_GLIBCXX_USE_CXX11_ABI=0")
ADD_EXECUTABLE(insert "insert.cc")
ADD_EXECUTABLE(get "get.cc")
ADD_EXECUTABLE(gtest_auto "gtest_auto.cpp")

TARGET_LINK_LIBRARIES(insert libdtc.2.so)
TARGET_LINK_LIBRARIES(get libdtc.2.so)
TARGET_LINK_LIBRARIES(gtest_auto gtest gmock libdtc.2.so pthread)




