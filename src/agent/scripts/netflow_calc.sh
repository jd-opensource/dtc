#!/bin/bash


usage() {
        echo "Useage : $0"
        echo "eg. sh $0 eth0 2"
        exit 1
}

if [ $# -lt 2 ]
then
        usage
fi

eth=$1
timer=$2

in_old=$(cat /proc/net/dev | grep $eth | sed -e "s/\(.*\)\:\(.*\)/\2/g" | awk '{print $1 }')
out_old=$(cat /proc/net/dev | grep $eth | sed -e "s/\(.*\)\:\(.*\)/\2/g" | awk '{print $9 }')

while true
do
        sleep ${timer}
        in=$(cat /proc/net/dev | grep $eth | sed -e "s/\(.*\)\:\(.*\)/\2/g" | awk '{print $1 }')
        out=$(cat /proc/net/dev | grep $eth | sed -e "s/\(.*\)\:\(.*\)/\2/g" | awk '{print $9 }')
        dif_in=$(((in-in_old)/timer))
        dif_in=$((dif_in/1024))
        dif_out=$(((out-out_old)/timer))
        dif_out=$((dif_out/1024))
        ct=$(date +"%F %H:%M:%S")
        echo "${ct} -- IN: ${dif_in} KByte/s     OUT: ${dif_out} KByte/s"
        in_old=${in}
        out_old=${out}
done

exit 0
