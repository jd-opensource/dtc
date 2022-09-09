#!/bin/sh

ulimit -c unlimited

DTC_BIN="agent-watchdog"

if [ "$1" = "stop" ] ; then
    pkill -9 $DTC_BIN
    pkill -9 dtcagent
    pkill -9 async-conn
    pkill -9 data-lifecycle
    ../sharding/bin/stop.sh
elif [ "$1" = "restart" ]; then
    pkill -9 $DTC_BIN
    pkill -9 dtcagent
    pkill -9 async-conn
    pkill -9 data-lifecycle
    ../sharding/bin/stop.sh
    sleep 2
    ./$DTC_BIN  >> /dev/null 2>&1
elif [ "$1" = "start" ]; then
    ./$DTC_BIN  >> /dev/null 2>&1 &
    sleep 1
else
    echo "usage: $0 stop | start |restart"
fi
