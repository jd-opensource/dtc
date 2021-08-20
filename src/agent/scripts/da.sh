#!/bin/bash
ulimit -c unlimited

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)"
PID_FILE="$BASE_DIR/../bin/da_pid"
LOG_LEVEL=3

START=0
STOP=0
IMMEDIATE=0
RELOAD=0
TEST_CONF=0
SHOWHELP=0

TEMP=`getopt -o v:p: -- "$@"` 
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi
eval set -- "$TEMP"

while true ; do
        case "$1" in
                -v)
                    echo "Option v, verbose: $2"; LOG_LEVEL=$2; shift 2 ;;
                -p)
                	echo "Option p, pid file: $2"; PID_FILE=$2; shift 2 ;;
                --) shift ; break ;;
                *) echo "Internal error!" ; exit 1 ;;
        esac
done

if [ "$1" = "start" ];then
	START=1
elif [ "$1" = "stop" ];then
      if [ "$2" = "normal" ] || [ "$2" = ""  ];then
      	  STOP=1
      elif [ "$2" = "immediate" ];then
      	  STOP=1
      	  IMMEDIATE=1
      else
    	  echo "error input argument"
    	  exit 1
       fi
elif [ "$1" = "reload" ];then
	RELOAD=1
elif [ "$1" = "testconf" ];then
	TEST_CONF=1
elif [ "$1" = "help" ];then
	SHOWHELP=1
else
	echo "error input argument"
	exit 1
fi

#test agent conf
if [ "$TEST_CONF" = "1" ];then
	echo "test dtcagent conf start......"
	$BASE_DIR/dtcagent -t
	echo "test conf success"
	exit 0
fi

#start dtcagent
if [ "$START" = "1" ];
then
	echo "dtcagent starting......"
	NEED_START=0;
	if [ -f "$PID_FILE" ];then
		DA_PID=`cat $PID_FILE`
		if test $( pgrep -l dtcagent | grep $DA_PID | wc -l ) -gt 0
		then
			echo "$BASE_DIR/dtcagent whth pid file[$PID_FILE] already run in $DA_PID"
			exit 0
		else
			NEED_START=1
		fi
	else
		NEED_START=1
	fi
	if [ "$NEED_START" = "1" ];then
		$BASE_DIR/dtcagent -v $LOG_LEVEL -d -p $PID_FILE >> /dev/null 2>&1
		sleep 1
		if [ ! -f "$PID_FILE" ]; then
			echo "$BASE_DIR/dtcagent start failed,please check error log"
			exit 1
		else
			DA_PID=`cat $PID_FILE` 
			if test $( pgrep -l dtcagent | grep $DA_PID | wc -l ) -lt 1
			then
				echo "$BASE_DIR/dtcagent start failed,please check error log"
				exit 1
			else
				echo "$BASE_DIR/dtcagent whth pid file[$PID_FILE] start success,pid is $DA_PID"
				echo "dtcagent has started successfully!"
				exit 0
			fi
		fi
	fi
fi

#stop dtcagent
if [ "$STOP" = 1 ];then
	if [ ! -f "$PID_FILE" ];then
		echo "$BASE_DIR/dtcagent stop failed, pid file[$PID_FILE] not exist"
		exit 1
	else
		DA_PID=`cat $PID_FILE`
		if test $( pgrep -l dtcagent | grep $DA_PID | wc -l ) -lt 1
		then
			echo "$BASE_DIR/dtcagent has stoped ,$DA_PID not existed"
			exit 0
		else
			if [ "$IMMEDIATE" = 1 ]
			then
				echo "stop immediate......."
				kill -9 $DA_PID
				sleep 1
				if test $( pgrep -l dtcagent | grep $DA_PID | wc -l ) -lt 1
				then
					echo "stop immediate success"
					echo "dtcagent has stoped successfully!"
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
					if test $( pgrep -l dtcagent | grep $DA_PID | wc -l ) -lt 1
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
					echo "dtcagent has stoped successfully!"
					exit 0
				else
					echo "normal stop failed,please check log"
					exit 1
				fi
			fi
		fi
	fi
fi

#hot restart dtcagent
if [ "$RELOAD" = "1" ];then
	if [ ! -f "$PID_FILE" ];then
		echo "$BASE_DIR/dtcagent stop failed, pid file[$PID_FILE] not exist"
		exit 1
	else
		DA_PID=`cat $PID_FILE`
		if test $( pgrep -l dtcagent | grep $DA_PID | wc -l ) -lt 1
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
						if test $( pgrep -l dtcagent | grep $NEW_DA_PID | wc -l ) -lt 1
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
				echo "dtcagent has restart successfully!"
				exit 0
			else
				echo "reload failed,please check error log"
				exit 1
			fi
		fi
	fi
fi
