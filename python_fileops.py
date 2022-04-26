import os
import argparse
import time
from collections import OrderedDict
import subprocess
import string
import sys
dir = "/home/cbs/"

def fileops(vol_name):
    print("Calling Fileoperations")
    chp = os.system('sudo chmod 777 -R '+dir+vol_name)
    chd = os.chdir(dir+vol_name)
    chd1=os.getcwd()
    rundd=os.system('sudo dd if=/dev/zero of='+dir+vol_name+'"/" file-1113 bs=1024 count=1024 2>&1')
    print('sudo dd if=/dev/zero of='+dir+vol_name+'"/" file-1113 bs=1024 count=1024 2>&1')
    if(rundd == 0):
        print("dd command Ran successful on ",vol_name)
    runfio=os.system("sudo fio --direct=1 --ioengine=libaio --eta-newline=1 --fallocate=none --filename="+dir+vol_name+"/test-file-1.txt"+" --size=1MB --bs=1K --rw=randrw --time_based=0 --runtime=1 --iodepth=1 --numjobs=1 --name=sysqa-data --group_reporting=1")
    print("sudo fio --direct=1 --ioengine=libaio --eta-newline=1 --fallocate=none --filename="+dir+vol_name+"/test-file-1.txt"+" --size=1MB --bs=1K --rw=randrw --time_based=0 --runtime=1 --iodepth=1 --numjobs=1 --name=sysqa-data --group_reporting=1")
    if(runfio == 0):
        print("fio command Ran successful on ",vol_name)
    print("End fileops")

def nfs_mount(vol_name,ip_add,protocol):
    print("Calling Mount function")
    checkdir = os.path.exists(dir+vol_name)
    if(checkdir == True):
        print("directory exists")
        ismount = os.path.ismount(dir+vol_name)
        if(ismount == False):
            print("volume is not mounted")
            mountnfs = os.system('sudo mount -t nfs -o rw,soft,rsize=65536,wsize=65536,vers='+protocol+',tcp '+ip_add+':'+vol_name+' '+dir+vol_name)
            print("sudo mount -t nfs -o rw,hard,rsize=65536,wsize=65536,vers="+protocol+",tcp "+ip_add+":"+vol_name+" "+dir+vol_name)
            if(mountnfs == 0):
                print("Volume is now mounted")
            else:
                print("error mounting volume")
        else:
            print("Volume already mounted")
    else:
        print("no directory exists")
        makedir = os.mkdir(dir+vol_name,mode = 0o777)
        chmod = os.chmod(dir+vol_name, 0o777)
        if(makedir == None):
            print("Directory Created successfully")
            mountnfs = os.system('sudo mount -t nfs -o rw,soft,rsize=65536,wsize=65536,vers='+protocol+',tcp '+ip_add+':'+vol_name+' '+dir+vol_name)
            if(mountnfs == 0):
                print("Volume is now mounted")
            else:
                print("error mounting volume")
        else:
            print("Unable to Create directory")

def nfs_umount(vol_name):
    print("in nfs unmount")
    chd = os.chdir(dir)
    ismount = os.path.ismount(dir+vol_name)
    if(ismount == False):
        print("volume is not mounted")
    else:
        print("Volume is already mounted, unmounting the volume",vol_name)
        umountnfs = os.system('sudo umount -f '+dir+vol_name+'')
        print('sudo umount -f '+dir+vol_name+'')
        if(umountnfs == 0):
            print("Volume is now un-mounted successfully")
        else:
            print("error un-mounting volume")
    remDir = os.rmdir(dir+vol_name)
    if(remDir == None):
        print("Volume directory removed successfully",vol_name)
    else:
        print("unable to delete directory:", vol_name)

def smb_mount(vol_name,ip_add):
    print("in smb mount")
    try:
        mount_smb = os.popen('net use * '+'\\'+'\\'+ip_add+'\\'+vol_name).read()
        print("MountSMB : ",mount_smb)
        if(len(mount_smb) == 0):
            print("Volume not mounted")
        else:
            print("Smb volume "+vol_name+" mounted successfully")
            list_smb_vol = os.popen('net use').read()
            print("SMB volume list : ",list_smb_vol)
            return mount_smb.split(" ")[1]

    except Exception as e:
        print(e)

def smb_umount(vol_name):
    print("in smb umount")
    try:
        list_smb = os.popen('net use | findstr '+vol_name).read()
        if [i for i in list_smb.split() if i != ""][1].startswith('\\'):
            print("Volume already unmounted")
        else:
            drive = [i for i in list_smb.split() if i != ""][1]
            umount_smb = os.popen('net use '+drive+' /delete').read()
            if(len(umount_smb) == 0):
                print("Volume not unmounted")
            else:
                print("Smb volume unmounted successfully")
                list_smb_vol = os.popen('net use').read()
                print("SMB volume list : ",list_smb_vol)
    except Exception as e:
        print(e)

def smb_get_drive(vol_name):
    list_smb = os.popen('net use | findstr '+vol_name).read()
    if [i for i in list_smb.split() if i != ""][1].startswith('\\'):
        return "False"
    else:
        return [i for i in list_smb.split() if i != ""][1]

def smb_file_ops(vol_name,net_bios):
    drive_name = smb_get_drive(vol_name)
    if (drive_name == False):
        print("Volume already unmounted. Cannot perform fileops")
    else:
        os.system('net use '+drive_name+'\\'+'\\'+net_bios+'\\'+vol_name)
        got_to_drive = os.system(drive_name)
        if(got_to_drive == 0):
            file_size = 5000
            for i in range(1):
                file_name = "_".join(["testfile", string.digits])+".txt"
                file_cmd = "{}&&fsutil file createnew {} {}".format(drive_name,file_name,file_size)
                fileops = os.system(file_cmd)
                if(fileops == 0):
                    print("Fileops completed successfully for  ",file_name)
                else:
                    print("Error in fileops for ",file_name)
        else:
            print("Unable to got to drive : ",drive_name)

def help():
    print("Invalid parameters : Please check parameters and try again.")
    print("-v|--volume : volume name"+"\n"+"-ip : ip address"+"\n"+"-p|--protocol : protocol version"+"\n"+"-m|--mode : mode(m-mount,f-fileops,u-unmount)")
    print("Example -- python3 <filename> -v <vol_name> -ip <ip> -p <protocol> -m <mode>")


def get_args(args):
    parser = argparse.ArgumentParser(allow_abbrev=True)
    parser.add_argument('--volume', required=True)
    parser.add_argument('--protocol', required=True)
    parser.add_argument('--ip', required=True)
    parser.add_argument('--mode', default='mfu')
    args = parser.parse_args()
    params = OrderedDict([('volume',""),('ip',""),('protocol',"")])
    for arg in vars(args):
        if arg in ('-v', 'volume'):
            params['volume'] = getattr(args,arg)
        if arg in ('-i', 'ip'):
            params['ip'] = getattr(args,arg)
        if arg in ('-p', 'protocol'):
            params['protocol'] = getattr(args,arg)
        if arg in ('-m', 'mode'):
            params['mode'] = getattr(args,arg)
    main(params)

def main(*params):
    for volume_list in params:
        if('nfs' in volume_list['protocol']):
            if("3" in volume_list['protocol']):
                vers = "3"
            else:
                vers = "4.1"
            print("linux")
            if('m'in volume_list['mode']):
                nfs_mount(volume_list['volume'],volume_list['ip'],vers)
            if('f'in volume_list['mode']):
                fileops(volume_list['volume'])
            if('u'in volume_list['mode']):
                nfs_umount(volume_list['volume'])
            else:
                help()
        else:
            print("windows")
            if('m'in volume_list['mode']):
                smb_mount(volume_list['volume'],volume_list['ip'])
            if('f'in volume_list['mode']):
                smb_file_ops(volume_list['volume'],volume_list['ip'])
            if('u' in volume_list['mode']):
                smb_umount(volume_list['volume'])
            else:
                help()

if __name__ == "__main__":
    a=sys.argv
    get_args(a)
