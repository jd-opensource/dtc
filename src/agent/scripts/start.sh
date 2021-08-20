#!/bin/sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
#register crontab automatic
#BINDIR=$DIR/../bin 
#PROGRAM=$DIR/../bin/detect_agent.sh
#CRONTAB_CMD="*/1 * * * * cd $BINDIR && $PROGRAM > /dev/null 2>&1 &" 
#crontab -l > /tmp/crontab.tmp 
#echo "$CRONTAB_CMD" >> /tmp/crontab.tmp 
#crontab /tmp/crontab.tmp 
#COUNT=`crontab -l | grep $PROGRAM | grep -v "grep"|wc -l `
#if [ $COUNT -lt 1 ]; then
#	echo "fail to add crontab $PROGRAM"
#	exit 1
#fi
#start da
if [ "$1" = "stop" ] ; then
cd $DIR/../bin
    ./da.sh stop
elif [ "$1" = "restart" ]; then
	cd $DIR/../bin
	./da.sh reload
elif [ "$1" = "start" ]; then
    cd $DIR/../bin
    ./da.sh start	
else
    echo "usage: $0 stop | start |restart"
fi
