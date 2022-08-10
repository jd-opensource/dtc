confpath=/etc/dtc

echo $INPUT_GITHUB
if [$INPUT_GITHUB != ""]; then 
    echo "1111111111"
    cp $INPUT_GITHUB/dtc.yaml $confpath/dtc.yaml
    cp $INPUT_GITHUB/log4cplus.conf $confpath/log4cplus.conf
fi

echo "22222222"

/usr/local/dtc/dtcd -d