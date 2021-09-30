#!/bin/bash

PROCESS_DAEMON(){
    while :
    do
        sleep 5
    done
}

/usr/local/dtc/bin/dtcagent &
/usr/local/dtc/bin/dtcd &
PROCESS_DAEMON