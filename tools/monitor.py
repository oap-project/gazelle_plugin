import os
import pyhdfs
import subprocess
import sys
import time

from inspect import currentframe, getframeinfo
from pathlib import Path

clients=['sr124']
home = str(Path.home())
base_dir = 'profile'
local_profile_dir=home+"/"+base_dir
hdfs_address='10.1.0.24:50070'

def killsar():
    for l in clients:
        try:
            cmd="ssh "+l+" ps aux | grep -w sar | grep -v grep | tr -s ' ' | cut -d' ' -f2"
            out=subprocess.check_output(cmd).decode('ascii').strip().split("\n")
            for x in out:
                cmd="ssh "+l+" kill "+x+" > /dev/null 2>&1"
                subprocess.call(cmd,shell=True)
        except Exception as e:
            print(e)
            pass
    for l in clients:
        try:
            cmd="ssh "+l+" ps aux | grep -w pidstat | grep -v grep | tr -s ' ' | cut -d' ' -f2"
            out=subprocess.check_output(cmd, shell=True).decode('ascii').strip().split("\n")
            for x in out:
                cmd="ssh "+l+" kill "+x+" > /dev/null 2>&1"
                subprocess.call(cmd,shell=True)
        except Exception as e: 
            print(e)
            pass
    for l in clients:
        try:
            cmd="ssh "+l+" ps aux | grep -w perf | grep -v grep | tr -s ' ' | cut -d' ' -f2"
            out=subprocess.check_output(cmd,shell=True).decode('ascii').strip().split("\n")
        except Exception as e:
            print(e)
            pass

def startmonitor(appid, **kwargs):
    print("[monitor.py]Starting system monitoring ...")
    print(clients)
    appid_profile_dir=local_profile_dir+"/"+appid
    cmd="mkdir -p "+appid_profile_dir
    print("Launching CMD create application id profile dir: %s" % cmd)
    subprocess.call(cmd,shell=True)

    for l in clients:
        cmd="ssh "+l+" date"
        print(subprocess.check_output(cmd,shell=True).decode('ascii'))

    killsar()

    for l in clients:
        print("[monitor.py]create profile directory")
        client_profile_dir=appid_profile_dir+"/"+l
        cmd="mkdir -p "+client_profile_dir
        print("[monitor.py]Launching CMD create server profile dir: %s" % cmd)
        subprocess.call(cmd,shell=True)
        cmd="ssh "+l+" mkdir -p "+client_profile_dir
        print("[monitor.py]Launching CMD create client profile dir: %s" % cmd)
        subprocess.call(cmd,shell=True)
        cmd="ssh "+l+" sar -o "+client_profile_dir+"/sar.bin -r -u -d -B -n DEV 1 >/dev/null 2>&1 &"
        print("[monitor.py]Launching CMD create sar.bin file: %s" % cmd)
        subprocess.call(cmd,shell=True)
        if kwargs.get("collect_pid",False):
            cmd="ssh "+l+" jps | grep CoarseGrainedExecutorBackend | head -n 1 | cut -d' ' -f 1 | xargs  -I % pidstat -h -t -p % 1  > "+client_profile_dir+"/pidstat.out 2>/dev/null &"
            print("Launching CMD collect pid: %s" % cmd)
            subprocess.call(cmd,shell=True)
    return appid_profile_dir

def stopmonitor(appid, eventlogdir, basedif):

    appid_profile_dir=local_profile_dir+"/"+appid
    cmd="mkdir -p "+appid_profile_dir
    print("Launching CMD create application id profile dir: %s" % cmd)
    subprocess.call(cmd,shell=True)

    killsar()

    with open("%s/starttime" % appid_profile_dir,"w") as f:
        f.write("{:d}".format(int(time.time()*1000)))

    hadoophome=os.environ["HADOOP_HOME"]
    userlogdir="/opt/hadoop/yarn/logs"

    for l in clients:
        client_profile_dir=appid_profile_dir+"/"+l
        cmd="ssh "+l+" sar -f "+client_profile_dir+"/sar.bin -r > "+client_profile_dir+"/sar_mem.sar;sar -f "+client_profile_dir+"/sar.bin -u > "+client_profile_dir+"/sar_cpu.sar;sar -f "+client_profile_dir+"/sar.bin -d > "+client_profile_dir+"/sar_disk.sar;sar -f "+client_profile_dir+"/sar.bin -n DEV > "+client_profile_dir+"/sar_nic.sar;sar -f "+client_profile_dir+"/sar.bin -B > "+client_profile_dir+"/sar_page.sar;"
        print("Launching CMD: %s" % cmd)
        subprocess.call(cmd,shell=True)
        cmd="ssh "+l+" grep -rI xgbtck --no-filename "+userlogdir+"/"+appid+"/* | sed 's/^ //g'  > "+client_profile_dir+"/xgbtck.txt"
        print("Launching CMD: %s" % cmd)
        subprocess.call(cmd,shell=True)
        cmd="scp -r "+l+":"+client_profile_dir+" "+appid_profile_dir+"/ > /dev/null 2>&1"
        print("Launching CMD: %s" % cmd)
        subprocess.call(cmd, shell=True)
        cmd="ssh "+l+" jps | grep CoarseGrainedExecutorBackend | head -n 2 | tail -n 1 | cut -d' ' -f 1  | xargs -I % ps -To tid p % > "+client_profile_dir+"/sched_threads.txt"
        subprocess.call(cmd, shell=True)
        cmd="ssh "+l+" sar -V > "+client_profile_dir+"/sarv.txt"
        print("Launching CMD: %s" % cmd)
        subprocess.call(cmd, shell=True)
        cmd="test -f "+client_profile_dir+"/perfstat.txt && head -n 1 "+client_profile_dir+"/perfstat.txt > "+client_profile_dir+"/perfstarttime"
        print("Launching CMD: %s" % cmd)
        subprocess.call(cmd,shell=True)

    logfile=eventlogdir+"/"+appid
    cmd="hadoop fs -copyToLocal "+logfile+" "+appid_profile_dir+"/app.log"
    print("Launching CMD hadoop fs copytolocal: %s" % cmd)
    subprocess.call(cmd,shell=True)

    fs = pyhdfs.HdfsClient(hosts=hdfs_address, user_name='root')

    print("Launching CMD hadoop fs -mkdir /%s" % basedif)
    fs.mkdirs("/" + basedif + "/")
    v=[os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser(local_profile_dir+"/"+appid)) for f in fn]
    for f in v:
        paths=os.path.split(f)
        fs.mkdirs("/"+ basedif + paths[0][len(local_profile_dir):])
        fs.copy_from_local(f,"/"+ basedif + paths[0][len(local_profile_dir):]+"/"+paths[1],overwrite=True)


if __name__ == '__main__':
    if sys.argv[1]=="start":
        startmonitor( sys.argv[2])
    elif sys.argv[1]=="stop":
        import datetime
        from datetime import date
        basedir=base_dir+"/"+date.today().strftime("%Y_%m_%d")
        stopmonitor( sys.argv[2],sys.argv[3],basedir)

        lastnightrun=["","",""]
        with open("log/runs.txt") as f:
            for l in f.readlines():
                x=l.strip().split(" ")
                if ( x[0]=="05" and x[2]!=sys.argv[2] ) or ( len(sys.argv)==5 and x[2]==sys.argv[4] ):
                    lastnightrun[0]=x[0]
                    lastnightrun[1]=x[1]
                    lastnightrun[2]=x[2]
        os.system(("./post_process.sh {} {} {} {}").format(date.today().strftime("%Y_%m_%d"), sys.argv[2], lastnightrun[1],lastnightrun[2]))

