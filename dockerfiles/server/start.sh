#!/bin/bash

INIT(){
    cd /usr/local/dtc/bin
    chmod +x dtcd
    ./dtcd.sh start
}

PROCESS_DAEMON(){
    while :
    do
        sleep 60
    done
}

INIT
PROCESS_DAEMON