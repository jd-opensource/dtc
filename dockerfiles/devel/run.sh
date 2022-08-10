#!/bin/bash

sleep_count=0
while (($sleep_count <= 100))
do
    if [ -f "/etc/dtc/dtc.yaml" ]; then 
        echo "Start running process."
        /usr/local/dtc/dtcd -d
    else
        sleep 1
        (($sleep_count++))
    fi
done
echo "Timeout waitting for dtc conf files."