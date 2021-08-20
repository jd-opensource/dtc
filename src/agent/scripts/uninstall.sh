#!/bin/sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
#PROGRAM=$DIR/../bin/detect_agent.sh
#TEMPFILE=/tmp/crontab.tmp
#if [ ! -f "TEMPFILE" ]; then 
#	/bin/rm -rf  $TEMPFILE
#fi 
#crontab -l | grep -v "grep" | grep -v "$PROGRAM"> $TEMPFILE
#crontab $TEMPFILE
cd $DIR/../bin
    ./da.sh stop
echo "uninstall successfully"