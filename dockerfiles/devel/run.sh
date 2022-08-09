confpath=/etc/dtc

if [$INPUT_GITHUB != ""]; then 
    cp $INPUT_GITHUB/dtc.yaml $confpath/dtc.yaml
    cp $INPUT_GITHUB/log4cplus.conf $confpath/log4cplus.conf
fi

/usr/local/dtc/dtcd -d