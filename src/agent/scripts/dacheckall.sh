#!/bin/sh

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
for dir_module in `ls $BASE_DIR`
do
	cd $BASE_DIR
	if [ -d $dir_module ]; then
		MODULE_DIR=$BASE_DIR/$dir_module
		cd $MODULE_DIR
		for dir_port in `ls`
			do
				cd $MODULE_DIR
				if [ -d $dir_port ];then
					INST_DIR=${MODULE_DIR}/${dir_port}
					cd $INST_DIR/bin
					su -l dtcadmin -m -c "cd ${INST_DIR}/bin ; ./detect_agent.sh > /dev/null 2>&1"
					
				fi
			done
	fi
done
