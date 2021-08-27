ADD_SUBDIRECTORY(./agent)
ADD_SUBDIRECTORY(./configcenter)
ADD_SUBDIRECTORY(./libs)	
ADD_SUBDIRECTORY(./devel)
ADD_SUBDIRECTORY(./daemons)
ADD_SUBDIRECTORY(./core)
#add_subdirectory(./connector)


#想卸载make install安装的库，进入到build文件夹，执行
# cat install_manifest.txt | sudo xargs rm