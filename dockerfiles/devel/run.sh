#!/bin/bash

sleep_count=0

while [ $sleep_count -le 100 ]
do
    if [ -f "/etc/dtc/dtc.yaml" ]; then 
        echo "Start running process."$1
        if [[ $1 == "dtc" ]]; then
            echo "entry dtc"
            /usr/local/dtc/dtcd -d
        elif [[ $1 == "agent" ]]; then
            echo "entry agent"
            /usr/local/dtc/agent
        else
            echo "exiting..."$1
            exit 0
        fi
    else
        echo "sleeping"$sleep_count
        sleep 1s
        let sleep_count+=1
    fi
done

echo "Timeout waitting for dtc conf files."