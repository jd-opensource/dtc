#!/bin/bash

INIT(){
    cd /usr/local/dtc/bin
    chmod +x dtcd
    ./dtcd
}

PROCESS_DAEMON(){
    while :
    do
        sleep 60
    done
}

INIT
PROCESS_DAEMON