#!/bin/bash

INIT(){
    cd /usr/local/sdk-cpp/
    chmod +x build.sh
    echo "=============BUILD==================="
    ./build.sh
    echo "=============INSERT=================="
    ./insert
    echo "=============GET====================="
    ./get
    echo "=============END====================="
}

PROCESS_DAEMON(){
    while :
    do
        sleep 60
    done
}

INIT
#PROCESS_DAEMON