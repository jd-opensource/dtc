#!/bin/bash

sleep_count=0

while [ $sleep_count -le 100 ]
do
    if [ -f "/etc/dtc/dtc.yaml" ]; then 
        echo "Start running process."$DTC_BIN","$DTC_ARGV
        /usr/local/dtc/$DTC_BIN $DTC_ARGV
    else
        echo "sleeping"$sleep_count
        sleep 1s
        let sleep_count+=1
    fi
done

echo "Timeout waitting for dtc conf files."