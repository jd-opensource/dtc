#!/bin/bash

sleep_count=0

while [ $sleep_count -le 100 ]
do
    if [ -f "/etc/dtc/dtc.yaml" ]; then 
        echo "Start running process."
        echo $1
        if [ $1 == "dtc"]; then
            /usr/local/dtc/dtcd -d
        elif [ $1 == "agent"]; then
            /usr/local/dtc/agent
        else
            exit 0
        fi
    else
        echo "sleeping"
        sleep 1s
        let sleep_count+=1
    fi
done

echo "Timeout waitting for dtc conf files."