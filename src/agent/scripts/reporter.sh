#!/bin/bash
ulimit -c unlimited

ROOT=$(cd $(dirname $0); pwd)
BINARY_NAME=da_reporter
BINARY_PATH=$ROOT
BINARY=$BINARY_PATH/$BINARY_NAME
PID_FILE=$ROOT/reporter_pid
LOG_LEVEL=3

if [ "$1" = "start" ];then
	echo "$BINARY start......"
	if [ -f "$PID_FILE" ];then
		DA_PID=`cat $PID_FILE`
		if test $( pgrep -l $BINARY_NAME | grep $DA_PID | wc -l ) -gt 0
		then
			echo "$BINARY_NAME with pid file[$PID_FILE] already run in $DA_PID"
			exit 0
		else
			NEED_START=1
		fi
	else
		NEED_START=1
	fi
	if [ "$NEED_START" = "1" ];then
		$BINARY -v $LOG_LEVEL -p $PID_FILE >> /dev/null 2>&1
		sleep 1
		if [ ! -f "$PID_FILE" ]; then
			echo "$BINARY start failed,please check error log"
			exit 1
		else
			DA_PID=`cat $PID_FILE` 
			if test $( pgrep -l $BINARY_NAME | grep $DA_PID | wc -l ) -lt 1
			then
				echo "$BINARY start failed,please check error log"
				exit 1
			else
				echo "$BINARY with pid file[$PID_FILE] start success,pid is $DA_PID"
				echo "$BINARY_NAME has started successfully!"
				exit 0
			fi
		fi
	fi
elif [ "$1" = "stop" ];then
	if [ ! -f "$PID_FILE" ];then
		echo "$BINARY stop failed, pid file[$PID_FILE] not exist"
		exit 1
	else
		DA_PID=`cat $PID_FILE`
		if test $( pgrep -l $BINARY_NAME | grep $DA_PID | wc -l ) -lt 1
		then
			echo "$BINARY has stoped ,$DA_PID not existed"
			exit 0
		else
			if [ "$IMMEDIATE" = 1 ]
			then
				echo "stop immediate......."
				kill -9 $DA_PID
				sleep 1
				if test $( pgrep -l $BINARY_NAME | grep $DA_PID | wc -l ) -lt 1
				then
					echo "stop immediate success"
					echo "$BINARY_NAME has stoped successfully!"
					exit 0
				else
					echo "stop immediate failed"
					exit 1
				fi
			else
				echo "normal stoping ......"
				STATUS=0
				kill -s	SIGTERM $DA_PID
				RETRY_COUNT=0
				while true;do
					if test $( pgrep -l $BINARY_NAME | grep $DA_PID | wc -l ) -lt 1
					then
						STATUS=1
						break
					fi
					if [ $RETRY_COUNT -eq 15 ]
					then
						break
					fi
					sleep 1
					let "RETRY_COUNT=$RETRY_COUNT+1"
				done
				if [ "$STATUS" = "1" ]
				then
					echo "normal stop success"
					echo "$BINARY_NAME has stoped successfully!"
					exit 0
				else
					echo "normal stop failed,please check log"
					exit 1
				fi
			fi
		fi
	fi
elif [ "$1" = "reload" ];then
	if [ ! -f "$PID_FILE" ];then
		echo "$BINARY stop failed, pid file[$PID_FILE] not exist"
		exit 1
	else
		DA_PID=`cat $PID_FILE`
		if test $( pgrep -l $BINARY_NAME | grep $DA_PID | wc -l ) -lt 1
		then
			echo "pid: $DA_PID not existed,agent has stoped"
			exit 1
		else
			echo "reloading......"
			STATUS=0
			kill -s SIGUSR1 $DA_PID
			while true;do
				if [ ! -f "$PID_FILE" ];then
					echo "reload failed,please check error log"
					break
				else
					NEW_DA_PID=`cat $PID_FILE`
					if [ $NEW_DA_PID != $DA_PID ]
					then
						if test $( pgrep -l $BINARY_NAME | grep $NEW_DA_PID | wc -l ) -lt 1
						then
							break
						else
							STATUS=1
							break
						fi
					fi
				fi
				sleep 1
			done
			if [ "$STATUS" = "1" ]
			then
				echo "reload success"
				echo "$BINARY_NAME has restart successfully!"
				exit 0
			else
				echo "reload failed,please check error log"
				exit 1
			fi
		fi
	fi
elif [ "$1" = "--help" ];then
	echo "$0 start|stop|reload"
elif [ "$1" = "-h" ];then
	echo "$0 start|stop|reload"
else
	echo "$0 start|stop|reload"
fi
