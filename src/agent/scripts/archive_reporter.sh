#!/bin/sh
git log -1 --pretty=format:%h 2>&1 1>/dev/null
if [ $? -ne 0 ] ; then
    echo This directory is not a git repository.
    exit 1
fi
git_version=$(git log -1 --pretty=format:%h)

root=$(cd $(dirname $0); pwd)

tar_dir_name=stats_reporter
tar_file_name=$tar_dir_name-$git_version.tgz

src_root=$root/../
dst_root=$root/$tar_dir_name
## destination subdirectories
dst_sub_dirs="$dst_root/bin $dst_root/log $dst_root/tool $dst_root/conf"

## source file array
src_files=($src_root/da_reporter $src_root/scripts/reporter.sh $src_root/scripts/check_reporter.sh $src_root/conf/reporter.xml)

## destination file array
dst_files=($dst_root/bin/da_reporter $dst_root/bin/reporter.sh $dst_root/bin/check_reporter.sh $dst_root/conf/reporter.xml)

if [ -d $dst_root ] ; then
	echo Please make sure that there is no directory named agent.
	exit 1
else
	mkdir -p $dst_root
	mkdir -p $dst_sub_dirs
fi

## copy admin files
src_file_count=${#src_files[*]}
dst_file_count=${#dst_files[*]}

for ((i=0; i<$src_file_count && i<$dst_file_count; i++))
{
	cp -f ${src_files[$i]}  ${dst_files[$i]}
}

## create archive
cd $root
tar -czf $tar_file_name $tar_dir_name 2>/dev/null
cd - > /dev/null

## delete temporary directory
rm -rf $dst_root

