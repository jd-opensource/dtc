CMAKE_MINIMUM_REQUIRED (VERSION 2.8)
PROJECT(dtc)
ADD_SUBDIRECTORY(./src)

#if you want to uninstall libraries, you should enter "build" folder, and use this command
# cat install_manifest.txt | sudo xargs rm