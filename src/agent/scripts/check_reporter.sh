#!/bin/sh

root=$(cd $(dirname $0); pwd)
binary=da_reporter
pid_file=$root/reporter_pid
controller=$root/reporter.sh

if [ ! -f $pid_file ]; then
	su -l dtcadmin -m -c "cd $root; $controller start > /dev/null 2>&1"
else
	pid=$(cat $pid_file)
	if [ $(pgrep -l $binary|grep $pid|wc -l) -lt 1 ]; then
		su -l dtcadmin -m -c "cd $root; $controller stop > /dev/null 2>&1; $controller start > /dev/null 2>&1"
	fi
fi

