#!/bin/bash

sleep_count=0

while [ $sleep_count -le 100 ]
do
    if [ -f "/etc/dtc/dtc.yaml" ]; then 
        echo "Start running process."
        /usr/local/dtc/dtcd -d
    else
        echo "sleeping"
        sleep 1s
        let sleep_count+=1
    fi
done

echo "Timeout waitting for dtc conf files."