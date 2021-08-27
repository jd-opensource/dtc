
ADD_SUBDIRECTORY(./agent)
ADD_SUBDIRECTORY(./configcenter)
ADD_SUBDIRECTORY(./libs)	
ADD_SUBDIRECTORY(./devel)
ADD_SUBDIRECTORY(./daemons)
ADD_SUBDIRECTORY(./core)
#ADD_SUBDIRECTORY(./connector)

#if you want to uninstall the library ,you should enter the build folder, and use this command.
# cat install_manifest.txt | sudo xargs rm