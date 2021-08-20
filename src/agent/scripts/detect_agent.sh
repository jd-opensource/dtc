#!/bin/bash

BASE_DIR=`dirname $0`/..
BASE_DIR=`(cd "$BASEDIR"; pwd)`
PID_FILE="$BASE_DIR/da_pid"
ALARM_URL=""
TOKEN=`pwd | awk  -F '/'  '{print $5}'`
PORT=`pwd | awk  -F '/'  '{print $6}'`
SPLIT="_"
ALARM_BODY_HEADER='req={"alarm_type":"weixin","app_name":"agent_monitor_dtc","data":{"alarm_content":"alarm_monitor:'
ALARM_BODY_END='_agent_abnormal_termination","alarm_list":"","alarm_title":"wsm"}}'
ALARM_INFO=${ALARM_BODY_HEADER}${TOKEN}${SPLIT}${PORT}${ALARM_BODY_END}

function report_error
{
	echo $1
	/usr/bin/curl -d $1 $2
	echo""
}

echo "pid file is $PID_FILE"
if [ ! -f "$PID_FILE" ]; 
then
		report_error  ${ALARM_INFO} ${ALARM_URL}
        $BASE_DIR/da.sh stop > /dev/null 2>&1
        $BASE_DIR/da.sh start > /dev/null 2>&1
        echo "pid file [$PID_FILE] is not exist,dtcagent been closed"
        exit 2
fi
da_pid=`cat $PID_FILE`
if test $( pgrep -l dtcagent | grep $da_pid | wc -l ) -lt 1
then
		report_error  ${ALARM_INFO} ${ALARM_URL}
        $BASE_DIR/da.sh stop > /dev/null 2>&1
        $BASE_DIR/da.sh start > /dev/null 2>&1
fi
