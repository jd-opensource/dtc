cmake_minimum_required (VERSION 2.8)
project (dtc)

add_subdirectory(./agent)
add_subdirectory(./configcenter)
add_subdirectory(./libs)	
add_subdirectory(./devel)
add_subdirectory(./daemons)
add_subdirectory(./core)
#add_subdirectory(./connector)


#想卸载make install安装的库，进入到build文件夹，执行
# cat install_manifest.txt | sudo xargs rm