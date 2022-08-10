confpath=/etc/dtc

echo $INPUT_GITHUB
if [ -d $INPUT_GITHUB ]; then 
    echo "Start copy conf files"
    cp $INPUT_GITHUB/dtc.yaml $confpath/dtc.yaml
    cp $INPUT_GITHUB/log4cplus.conf $confpath/log4cplus.conf

    netstat
    ping mysql

    echo "Start running process."
    /usr/local/dtc/dtcd -d
else
    echo "No conf file found in INPUT_GITHUB"
fi