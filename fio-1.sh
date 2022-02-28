#!/bin/bash

mode=$2
dir="/var/tmp/"
check=$?

mount1(){
echo "Calling Mount"
for key in "${volume_list[@]}"; do
    if [ -d "$dir/$key" ]; then
	echo "$dir/$key"
        rmdir "$dir/$key"
        echo "remove directory"
	if [ $check -eq 0 ]; then
	    echo "Dir $key removed successfully"
	    mkdir "$dir/$key"
	    if [ $check -eq 0 ]; then
		echo "Dir $key created successfully"
	        echo "Listing dir";
                ls "$dir" | grep "$key"
		echo "Dir $key exists"
	    fi
	fi
        echo "Mount volume"
        #sudo mount $key $dir/$key
        echo "sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers=3,tcp $protocol_list:/$key $dir/$key"
        #mount_vol=`sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers=3,tcp 10.193.224.218:/$key $dir/$key`
        mount_vol=`sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers=3,tcp ${protocol_list}:/${key} $
{dir}/${key}`
        echo $mount_vol
        if [ $check -eq 0 ];then
            vol_check=`df -h | grep "$key"`
	    if [ ! -z $vol_check ]; then
	        echo "volume mounted successfully"
            else
                echo "Unable to mount volume $key"
            fi
        fi
    else
	mkdir "/home/cbs/$key"
	if [ $check -eq 0 ]; then
	    echo "Dir $key created successfully"
	    echo "Listing dir";ls "$dir" | grep "$key"
	    echo "Dir $key exists"
        fi
        #mount_vol=`sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers=3,tcp 10.193.224.218:/$key $dir/$key`
        mount_vol=`sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers=3,tcp ${protocol_list}:/${key} $
{dir}/${key}`
        echo $mount_vol
        if [ $check -eq 0 ];then
            vol_check=`df -h | grep "$key"`
            if [ ! -z $vol_check ]; then
                echo "volume mounted successfully"
            else
                echo "Unable to mount volume"
            fi  
        fi

    fi
done
echo "End of Mout function"
}

umount1(){
echo "Calling Umount"
for key in "${volume_list[@]}"; do
    vol_check=`df -h | grep "$key"`
    echo $vol_check
    if [ ! -z "$vol_check" ]; then
        echo "Volume already mounted"
	umount_vol=`sudo umount -f $dir/$key`
	echo $umount_vol
        echo "volume unmounted successfully"
	if [ -d "$dir/$key" ]; then
            remove_dir=`rmdir $dir/$key`
	    echo $remove_dir
	    if [ $check -eq 0 ]; then
                echo "Dir $key removed successfully"
            fi
	fi	    
    else
        echo "Volume not mounted"
	if [ -d "$dir/$key" ]; then
            remove_dir=`rmdir $dir/$key`
            echo $remove_dir
	    if [ $check -eq 0 ]; then
                echo "Dir $key removed successfully"
            fi
        fi
    fi
done
echo "End of Umount function"
}


fileops(){
echo "Calling Fileoperations"
for key in "${volume_list[@]}"; do
    if [ -d "$dir/$key" ]; then
        chd='sudo cd $dir'
        chd1='pwd'
        echo $chd1
        rundd=`sudo dd if=/dev/zero of=$dir/$key/file-1113 bs=1024 count=1024 2>&1`
        if [ $check -eq 0 ]; then
            echo "dd command Ran successful on $key"
        fi
        runfio='sudo fio --direct=1 --ioengine=libaio --eta-newline=1 --fallocate=none --filename=/$dir/$key/test-file-1.txt --size=1MB --bs=1K --rw=randrw --time_based=0 --runtime=1 --iodepth=1 --numjobs=1 --name=sysqa-data --group_reporting=1'
        if [ $check -eq 0 ]; then
            echo "fio command Ran successful on $key"
        fi
    fi
done
}

Help()
{
	echo "Syntax: scriptName volName:protocolName [-m|f|u|h]"
	echo "options:"
	echo "-m    Mount the volumes."
	echo "-f    Perform fileops operation on the volumes."
	echo "-u    Unmount the volumes."
	echo "-h    Print the Help."
}


main()
{
    my_str="$1"  
    echo "$my_str"
    my_arr=($(echo $my_str | tr "," "\t"))
    volume_list=()
    protocol_list=()

    for element in "${my_arr[@]}"  
    do  
        demo_var=`echo $element | tr ":" "\n"`
        vol=`echo $demo_var | awk '{ print $1 }'`
        protocol=`echo $demo_var | awk '{ print $2 }'`
        volume_list+=($vol)
        protocol_list+=($protocol)
    done

    echo "List with volumes and protocols : ${my_arr[@]}"
    echo "Volume_list : ${volume_list[@]}"
    echo "Protocol_list : ${protocol_list[@]}"
    echo "end of input param1"
    echo $mode
    
    if [[ $mode != *[mfuh]* ]]; then
    #if [[ $mode != *"m"*"f"*"u"* ]];then
        echo "Invalid options. Please enter correct options and try again."
	Help
        exit
    else
        if [[ $mode == *"m"* ]]; then
            echo "It's mount"
    #       mount1 
        fi
        if [[ $mode == *"f"* ]]; then
            echo "It's fileops"
    #       fileops
        fi
        if [[ $mode == *"u"* ]]; then
            echo "It's unmount"
    #       umount1
        fi
        if [[ $mode == *"h"* ]]; then
            Help
        fi
   fi
}

main "$1" "$2"
