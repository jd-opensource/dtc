#!/bin/bash

sleep_count=0

while [ $sleep_count -le 100 ]
do
    if [ -f "/etc/dtc/dtc.yaml" ]; then 
        echo "Start running process."
        if [ $1 == "dtc"]; then
            /usr/local/dtc/dtcd -d
        elif [ $1 == "agent"]; then
            /usr/local/dtc/agent
        fi
    else
        echo "sleeping"
        sleep 1s
        let sleep_count+=1
    fi
done

echo "Timeout waitting for dtc conf files."