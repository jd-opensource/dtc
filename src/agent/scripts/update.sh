#!/bin/sh

root=$(cd $(dirname $0); pwd)

## source file path
src_root=$root/..

files="admin_script/start.sh
admin_script/update.sh
admin_script/uninstall.sh
bin/da.sh
bin/dtcagent
bin/detect_agent.sh
bin/version.txt
tools/netflow_calc.sh
tools/da_stats_tool
tools/da_conf_updater
"
conf_files="conf/agent.xml"
app_bash="bin/da.sh"

## backup suffix
suffix=pre_ver

function usage(){
    echo "Version 0.1"
    echo $(basename $0) "update|undo agent_root [agent_root...]"
    echo "    update:  update each agent path"
    echo "    undo:    undo each agent path"
    echo
}

function validate(){
    agent_root=$1
    for file in $files
    do
        [ ! -d $agent_root/$(dirname $file) ] && return 1
    done
    for conf in $conf_files
    do
        [ ! -d $agent_root/$(dirname $conf) ] && return 1
    done

    return 0
}

function reload(){
    now=`pwd`
    file=$1

    if [ -f $file ] ; then
        cd `dirname $file`
        bash `basename $file` reload
        cd $now
    fi
}

function backup(){
    agent_root=$1
    for file in $files
    do
        [ -f $agent_root/$file.$suffix ] && rm -f $agent_root/$file.$suffix
        [ -f $agent_root/$file ] && cp -f $agent_root/$file $agent_root/$file.$suffix
    done

    for conf in $conf_files
    do
        [ -f $agent_root/$conf ] && cp -f $agent_root/$conf $agent_root/$conf.$suffix
    done
    return 0
}

function update_conf(){
    agent_root=$1
    for file in $conf_files
    do
        sed -i "s/Backlog=\"500\"/Backlog=\"1024\"/" $agent_root/$file
        sed -i "s/Client_Connections=\"500\"/Client_Connections=\"30000\"/" $agent_root/$file

        $agent_root/tools/da_conf_updater $agent_root/$file

		if cat $agent_root/$file | grep TopPercentileDomain > /dev/null
		then
			:
		else
			#添加TP99配置项
			sed -i "s/<MODULE/<MODULE TopPercentileEnable=\"true\" TopPercentileDomain=\"127.0.0.1\" TopPercentilePort=\"20020\"/g" $agent_root/$file
		fi

		if cat $agent_root/$file | grep LOG_MODULE > /dev/null
		then
			:
		else
			#添加日志配置项
			sed -i "/<VERSION/a\\    <LOG_MODULE LogSwitch=\"0\" RemoteLogSwitch=\"1\" RemoteLogIP=\"127.0.0.1\" RemoteLogPort=\"9997\" \/>" $agent_root/$file
		fi
		
    done
    return 0
}

function update(){
    agent_root=$1
    if [ -d $agent_root ] ; then
        validate $agent_root
        if [ $? -ne 0 ] ; then
            echo Directory $agent_root is incorrect.
            return 1
        else
           backup $agent_root
           if [ $? -eq 0 ] ; then
               for file in $files
               do
                   cp -f $src_root/$file $agent_root/$file
               done

               for conf in $conf_files
               do
                   [ ! -f $agent_root/$conf ] && cp -f $src_root/$conf $agent_root/$conf
               done

               update_conf $agent_root && reload $agent_root/$app_bash
           fi
        fi
    else
        echo Directory $agent_root is not exist while updating.
        return 1
    fi
}

function undo(){
    agent_root=$1
    if [ -d $agent_root ] ; then
        validate $agent_root
        if [ $? -ne 0 ] ; then
            echo Directory $agent_root is incorrect.
            return 1
        else
            for file in $files
            do
                [ -f $agent_root/$file.$suffix ] && rm -f $agent_root/$file && mv -f $agent_root/$file.$suffix $agent_root/$file
            done
    
            for conf in $conf_files
            do
                [ ! -f $agent_root/$conf ] && mv -f $agent_root/$conf.$suffix $agent_root/$conf
            done
            reload $agent_root/$app_bash
            return 0
        fi
    else
        echo Directory $agent_root is not exist while undo.
        return 1
    fi
}

function do_each_update(){
    echo Begin to update...
    for agent_root in $*
    do
        update $agent_root
        if [ $? -eq 0 ] ; then
            echo "Update $agent_root success."
        else
            echo "Update $agent_root failed."
        fi
    done
    echo Update end.
}

function do_each_undo(){
    echo Begin to undo...
    for agent_root in $*
    do
        undo $agent_root
        if [ $? -eq 0 ] ; then
            echo "Undo $agent_root success."
        else
            echo "Undo $agent_root failed."
        fi
    done
    echo Undo end.
}

if [ $# -le 1 ] ; then
    usage
    exit 1
fi

case $1 in
    undo)
        shift 1
        do_each_undo $*
        ;;
    update)
        shift 1
        do_each_update $*
        ;;
    *)
        usage
        ;;
esac

