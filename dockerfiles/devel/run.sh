#!/bin/bash

sleep_count=0

while [ $sleep_count -le 100 ]
do
    if [ -f "/etc/dtc/dtc.yaml" ]; then 
        echo "Start running process: "$DTC_BIN","$DTC_ARGV1","$DTC_ARGV2","$DTC_ARGV3
        cd /usr/local/dtc/
        ./$DTC_BIN $DTC_ARGV1 $DTC_ARGV2 $DTV_ARGV3
        break
    else
        echo "sleeping: "$sleep_count"s"
        sleep 1s
        let sleep_count+=1
    fi
done

echo "Timeout waitting for dtc conf files."