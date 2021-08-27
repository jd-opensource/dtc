#!/bin/sh

ulimit -c unlimited

DTC_BIN="dtcd_docker"
rm -f "$DTC_BIN"
ln -s dtcd "$DTC_BIN" 

if [ "$1" = "stop" ] ; then
    killall -9 $DTC_BIN
elif [ "$1" = "restart" ]; then
    killall -9 $DTC_BIN
	sleep 2
    ./$DTC_BIN  >> /dev/null 2>&1
elif [ "$1" = "start" ]; then
    ./$DTC_BIN  >> /dev/null 2>&1
	sleep 1
else
    echo "usage: $0 stop | start |restart"
fi
