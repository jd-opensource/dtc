CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT(dtc)

#Use optional command `cmake -DCMAKE_TEST_OPTION=ON ..` to build test demo.
option(CMAKE_TEST_OPTION "option for test_demo" OFF)

ADD_SUBDIRECTORY(./src)

if(CMAKE_TEST_OPTION)
ADD_SUBDIRECTORY(./test)
SET(CMAKE_TEST_OPTION OFF CACHE BOOL INTERNAL FORCE)
ENDIF(CMAKE_TEST_OPTION)

#if you want to uninstall libraries, you should enter "build" folder, and use this command
# cat install_manifest.txt | sudo xargs rm