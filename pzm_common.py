#!/usr/bin/env -S python3 -u

import subprocess
import datetime
import sys

def initialize():
    global initialized
    global debug
    global test
    global statusJsonFile
    global considered_empty


    try:
        initialized
    except NameError:
        debug = False
        test = False


        statusJsonFile = "/var/lib/pve-zsync/manager_sync_state"


        considered_empty = ['\n', '', " "]
        initialized = True
    else:
        pass
 	#Initialized was defined, skip resetting values


#Log to stdout
def log(data):
    print("["+ datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")+"] " + str(data))

#Log to stdout if global debug variable is set
def log_debug(data):
    global debug
    if debug:
        log("DEBUG - " + str(data))

#Execute command will not alter anything. These commands can be executed as normal in "TEST" mode
def execute_readonly_command(command):
    log_debug ("Executing command: " + " ".join(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")
    return 0, "", ""

#Execute command which will definetly alter something. Will not be executed in "TEST" mode
def execute_command(command, shell=False):
    global test
    if test:
        log_debug ("Would execute command: " + " ".join(command))
    else:
        log_debug ("Executing command: " + " ".join(command))
    if not test:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
        stdout, stderr = process.communicate()
        return process.returncode, stdout.decode("utf-8"), stderr.decode("utf-8"), process.pid
    return 0, "", "", ""

#Get VM or CT ids from command. command will be either lxc or pct, including ids are numbers, excluding ids are numbers which were given with a heading minus
def get_ids(command, including, excluding):
    rc, stdout, stderr = execute_readonly_command([command, 'list'])

    id_lines = stdout.splitlines()
    if not id_lines:
        log_debug ("No IDs found with " + command + " list")
        return []

    id_lines.pop(0)
    existing_vmct_ids = []

    for line in id_lines:
        line_str = line.lstrip()
        id = str(line_str.split(' ',1)[0])
        existing_vmct_ids.append(id)

    backup_ids = []

    if including:
        for id in including:
            if id in existing_vmct_ids:
                backup_ids.append(existing_vmct_ids.pop(existing_vmct_ids.index(id)))
            if ':' in id: #Pull
                backup_ids.append(id)
    elif excluding:
        backup_ids = existing_vmct_ids
        for id in excluding:
            if id in backup_ids:
                backup_ids.pop(backup_ids.index(id))
    else:
        backup_ids = existing_vmct_ids
    return backup_ids

#Check if ZFS pool exists on the remote side
def check_zfs_pool(hostname,zfspool):
    rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'zfs' ,'list', '-rH', '-o', 'name'])
    if stderr != "":
        log ("(SSH) Error while getting zfs list names " + stderr)
        sys.exit(1)
    else:
        if zfspool not in stdout:
            log ("ZFS Pool " + zfspool + " does not exist on " + hostname)
            sys.exit(1)
        else:
            return stdout

