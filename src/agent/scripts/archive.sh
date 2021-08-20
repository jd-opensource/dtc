#!/bin/sh
git log -1 --pretty=format:%h 2>&1 1>/dev/null
if [ $? -ne 0 ] ; then
    echo This directory is not a git repository.
    exit 1
fi

git_version=$(git log -1 --pretty=format:%h)
tar_dir_name=agent
tar_file_name=agent-$git_version.tgz

root=$(cd $(dirname $0); pwd)

dst_root=$root/$tar_dir_name
src_root=$root/../
tar_sub_dirs="$dst_root/admin_script $dst_root/bin $dst_root/conf $dst_root/tools $dst_root/log $dst_root/stats"

## source file path
src_start_sh=$src_root/scripts/start.sh
src_update_sh=$src_root/scripts/update.sh
src_uninstall_sh=$src_root/scripts/uninstall.sh
src_da_sh=$src_root/scripts/da.sh
src_dtcagent=$src_root/dtcagent
src_detect_agent_sh=$src_root/scripts/detect_agent.sh
src_agent_xml=$src_root/conf/agent.xml
src_da_stats_tool=$src_root/da_stats_tool
src_da_hash_tool=$src_root/da_hash_tool
src_netflow_calc_sh=$src_root/scripts/netflow_calc.sh
src_da_conf_updater=$src_root/da_conf_updater

## destination file path
dst_start_sh=$dst_root/admin_script/start.sh
dst_update_sh=$dst_root/admin_script/update.sh
dst_uninstall_sh=$dst_root/admin_script/uninstall.sh
dst_da_sh=$dst_root/bin/da.sh
dst_dtcagent=$dst_root/bin/dtcagent
dst_detect_agent_sh=$dst_root/bin/detect_agent.sh
dst_agent_xml=$dst_root/conf/agent.xml
dst_da_stats_tool=$dst_root/tools/da_stats_tool
dst_da_hash_tool=$dst_root/tools/da_hash_tool
dst_netflow_calc_sh=$dst_root/tools/netflow_calc.sh
dst_da_conf_updater=$dst_root/tools/da_conf_updater
dst_version_file=$dst_root/bin/version.txt

if [ -d $dst_root ] ; then
	echo Please make sure that there is no directory named agent.
	exit 1
else
	mkdir -p $dst_root
	mkdir $tar_sub_dirs
fi

## dump version file
$src_dtcagent -V > $dst_version_file

## copy admin scripty files
cp -f $src_start_sh $dst_start_sh
cp -f $src_update_sh $dst_update_sh
cp -f $src_uninstall_sh $dst_uninstall_sh

## copy binary file
cp -f $src_da_sh $dst_da_sh
cp -f $src_dtcagent $dst_dtcagent
cp -f $src_detect_agent_sh $dst_detect_agent_sh

## copy configuration file
cp -f $src_agent_xml $dst_agent_xml

## copy tools
cp -f $src_da_stats_tool $dst_da_stats_tool
cp -f $src_da_hash_tool $dst_da_hash_tool
cp -f $src_netflow_calc_sh $dst_netflow_calc_sh
cp -f $src_da_conf_updater $dst_da_conf_updater

## create archive
cd $root
tar -czf $tar_file_name $tar_dir_name 2>/dev/null
cd - > /dev/null

## delete temporary directory
rm -rf $dst_root

