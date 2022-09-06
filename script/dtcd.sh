#!/bin/sh

ulimit -c unlimited

DTC_BIN="dtcd"

if [ "$1" = "stop" ] ; then
    pkill -9 $DTC_BIN
    pkill -9 connector
    pkill -9 hwcserver
elif [ "$1" = "restart" ]; then
    killall -9 $DTC_BIN
    pkill -9 connector
    pkill -9 hwcserver
	sleep 2
    ./$DTC_BIN  >> /dev/null 2>&1
elif [ "$1" = "start" ]; then
    ./$DTC_BIN  >> /dev/null 2>&1
	sleep 1
else
    echo "usage: $0 stop | start |restart"
fi

