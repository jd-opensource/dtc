#!/bin/bash

sleep_count=0

while [ $sleep_count -le 100 ]
do
    if [ -f "/usr/local/dtc/conf/dtc.yaml" ]; then 

        cd /usr/local/lib/
        ln -snf libdtc.so.1  libdtc.so
        ln -snf libdtcapi.so  libdtc.so.1

        echo "Start running process: "$DTC_BIN","$DTC_ARGV
        cd /usr/local/dtc/bin/
        ./$DTC_BIN $DTC_ARGV
        break
    else
        echo "sleeping: "$sleep_count"s"
        sleep 1s
        let sleep_count+=1
    fi
done
sleep 1000000s
echo "Timeout waitting for dtc conf files."