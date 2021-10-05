#!/usr/bin/env -S python3 -u

import sys
import argparse
import subprocess
import datetime
import os
import json
import time
import signal
import socket
import random
import re
from prettytable import PrettyTable
from json.decoder import JSONDecodeError

debug = False
test = False
locked = False
local_locked_here = False
remote_locked_here = False

statusJsonFile = "/var/lib/pve-zsync/manager_sync_state"
remoteSyncLock = "/var/lib/pve-zsync/manager_sync.lock"

logpath = "/var/log/pve-zsync"

#Log to stdout
def log(data):
    print("["+ datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")+"] " + str(data))

#Log to stdout if global debug variable is set
def log_debug(data):
    global debug
    if debug:
        log("DEBUG - " + str(data))

#Colors for fancy table output
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

considered_empty = ['\n', '', " "]

#Execute command will not alter anything. These commands can be executed as normal in "TEST" mode
def execute_readonly_command(command):
    log_debug ("Executing command: " + " ".join(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")
    return 0, "", ""

#Execute command which will definetly alter something. Will not be execute in "TEST" mode
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

#Check if the local lock (=file "remoteSyncLock") is lockable
#the file on remote and local is the same, in order to be able to do local and remote sync one by one.
def can_get_local_lock():
    global debug
    if os.path.exists(remoteSyncLock):
        with open(remoteSyncLock, 'r') as lockfile:
            output = lockfile.read()
            log ("Local lock is held by " + output + ", have to wait...")
        return False
    else:
        log_debug ("Local lockfile does not exist, can proceed...")
        return True

#Check if the remote lock (=file "remoteSyncLock") is lockable
#the file on remote and local is the same, in order to be able to do local and remote sync one by one.
def can_get_remote_lock(hostname):
    global locked
    lockvalue = socket.gethostname().lower() + "-" + str(os.getpid())
    rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'cat', remoteSyncLock])
    if rc == 1: #rc 1 means, file not found - which means no lock is held on remote side
        log_debug ("Remote lockfile does not exist, can proceed...")
        return True

    ################# REMOVED: It doesn't seem to be needed, as there is no possibility that the remote side could be locked by "us" (=hostname and pid identical) at this point in the execution
    ################# The local lock, which would be the exact same file as the remote lock in a local sync, get's set AFTER the remote file was placed

    #elif rc == 0 and lockvalue in stdout.lower(): #rc 0 - file found, if my hostname is the same as in the file, proceed
    #    #Needed for local syncs/backups
    #    log_debug ("Remote lock is held by this host and this process, can proceed...")
    #    return True


    elif rc == 0: #rc 0 file found, but content is not my hostname, can't proceed
        log ("Remote lock is held by " + stdout + ", have to wait...")
        return False
    else: #rc not 1 or 0 - must be another error - can't proceed
        log ("(SSH) Error while checking lock availability (Maybe host is down or network issue) " + stderr)
        return False

#Gather the local lock. Return true if we got it
def lock_local():
    global local_locked_here
    log_debug ("Locking locally")
    lockvalue = socket.gethostname().lower() + "-" + str(os.getpid())
    if not os.path.exists(remoteSyncLock): #In case it was locked by "remote lock" if it's a local sync, do nothing
        with open(remoteSyncLock, 'w') as lockfile:
            log_debug("Writing local lockfile")
            lockfile.write(lockvalue)
        local_locked_here = True
        execute_command(['chattr', '+i', remoteSyncLock]) #Make file immuteable with chattr
    else:
        with open(remoteSyncLock, 'r') as lockfile:
            output = lockfile.read()
        if lockvalue in output.lower(): #If read value is the same as this hostname-pid then it was us who locked the file. Most likely the remote lock part in a local sync
            local_locked_here = False
            log_debug ("Was already locked locally")
        else:
            local_locked_here = False
            log ("Couldn't get local lock as it's held by " + output + "!")
            return False
    log_debug ("Locally locked")
    return True

#Gather the remote lock. Return true if we got it. Also return true if the remote lock is held by this host and this PID. This will happen in a local sync situation
def lock_remote(hostname):
    global remote_locked_here
    log_debug("Locking remotly")
    log_debug("Trying to write remote lockfile")
    lockvalue = socket.gethostname().lower() + "-" + str(os.getpid())

    rc, stdout, stderr, pid = execute_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname,
                                               "echo -n " + lockvalue + " > " + remoteSyncLock + " && chattr +i " + remoteSyncLock])
    if rc == 1: #Operation not permitteed or File no Found in chattr


        ################# REMOVED: It doesn't seem to be needed, as there is no possibility that the remote side could be locked by "us" (=hostname and pid identical) at this point in the execution
        ################# The local lock, which would be the exact same file as the remote lock in a local sync, get's set AFTER the remote file was placed

        #rc, stdout, stderr_1 = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'cat', remoteSyncLock])
        #if lockvalue in stdout.lower():
        #    #Wasn't able to get lock, due to chattr imutability, but if lockvalue is ours, it count's as if.
        #    log_debug ("Was already remotely locked")
        #    remote_locked_here = False
        #    log_debug("Remotely locked")
        #    return True
        #else:
        #    log ("Wasn't able to get the remote lock! " + stderr)
        #    return False


        log ("Wasn't able to get the remote lock! " + stderr)
        return False
    elif rc == 0: #Worked fine
        remote_locked_here = True
        log_debug("Remotely locked")
        return True
    else:
        log ("(SSH) Error in putting lock on remote side, trying again. " + stderr)
        return False

#Release the remote lock
def unlock_remote(hostname):
    global locked
    global remote_locked_here
    if remote_locked_here: #Only delete if it was remote locked here
        while locked: #Make sure we safely delete the lock
            #chattr: make file muteable again
            log_debug("Removing remote lockfile")
            rc, stdout, stderr, pid = execute_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'chattr -i ' + remoteSyncLock + ' ; rm ' + remoteSyncLock])
            if rc == 0:
                locked = False
            elif rc == 1:
                log_debug ("(SSH) Odd, remote lockfile doesn't exist anymore???")
                locked = False
            else:
                log ("(SSH) Error while deleting the remote lock, trying again " + stderr)
                time.sleep(30)
    else:
        log_debug("Not removing remote lockfile as it wasn't created in lock_remote (was previously locked)")
        locked = False
    log_debug("Remotely unlocked")

#Release to local lock
def unlock_local():
    global locked
    global local_locked_here
    if local_locked_here: #Only delete if it was remote locked here
        log_debug("Removing local lockfile")
        if os.path.exists(remoteSyncLock):
            execute_command(['chattr', '-i', remoteSyncLock]) #Make file mutable again
            os.remove(remoteSyncLock)
        else:
            if socket.gethostname().lower() not in hostname.lower() and "localhost" not in hostname.lower():
                #AKA if my hostname is not the same as the hostname where the remotefile was previously removed. = Non-local Backup
                log_debug ("Odd, local lockfile doesn't exist anymore???")
                #It would be normal that the lockfile doesn't exist anymore at this point, if the destination was localhost
    else:
        log_debug("Not removing local lockfile as it wasn't created in lock_local (was previously locked)")

    log_debug ("Locally unlocked")
    if remote_locked_here or local_locked_here or locked:
        log ("Locks released")

#Check if both locks are available, then lock both. If anything goes wrong, reset and start over.
def lock(hostname):
    global locked
    global test
    presleep = random.uniform(0,60)
    if not test:
        log ("Waiting for " + str(presleep) + "s before starting...")
        time.sleep(presleep) #Random Delay to minimize possibility of simultanious locking...
    log ("Aquiring locks")
    while not locked: #Make sure lock was established successfully on remote side. If not, check again if possible
        while not (can_get_remote_lock(hostname) and can_get_local_lock()):
            sleeptime = random.uniform(30,60)
            log_debug ("Lock is held... sleeping " + str(sleeptime) + "s")
            time.sleep(sleeptime)
        if lock_remote(hostname):
            if not lock_local():
                log("Local lock couldn't get aquired, even if the prechecks said it would be. Unlocking remote, and trying again...")
                unlock_remote(hostname) #If for any reason we weren't able to get the local lock but did get the remote lock - Unlock the remote lock again
            else:
                locked = True #breaks the while loop
    log ("Locks aquired")

#Unlock remote and lock lock
def unlock(hostname):
    global locked
    global remote_locked_here
    global local_locked_here
    if remote_locked_here or local_locked_here or locked:
        log("Releasing locks")
    unlock_remote(hostname)
    unlock_local()

#Get VM or CT ids from command. command will be either lxc or pct, including ids are numbers, excluding ids are numbers which were given with a heading minus
def get_ids(command, including, excluding):
    global debug
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


#Removed CT/VM IDs which no longer exist from the status file.
def cleanup_json(delete = ""):
    if not os.path.exists(statusJsonFile):
        os.mknod(statusJsonFile)
    with open(statusJsonFile, "r") as jsonFile:
        try:
            data = json.load(jsonFile)
        except JSONDecodeError:
            data = {}
            pass
        vmids = get_ids("qm",[],[])
        ctids = get_ids("pct",[],[])
        ids = vmids + ctids
        if delete != "":
            ids.remove(delete)
        newData = {}
        for name, data in data.items():
            if (data['id'] in ids) or (data['id'] == "all") or ':' in data['id']:
                 newData[data['id'] + "_" + data['backupname']] = {
                     'id': data['id'],
                     'backupname': data['backupname'],
                     'starttime': data['starttime'],
                     'endtime': data['endtime'],
                     'duration': data['duration'],
                     'size': data['size'] if data.get('size') is not None else "-",
                     'status': data['status'],
                     'info': data['info']
                 }
        with open(statusJsonFile, "w") as jsonFile:
            json.dump(newData, jsonFile)

#Delete logfiles from errored syncs if they are older than 7 days.
def cleanup_logfolder():
    if not os.path.exists(logpath):
        return
    for f in os.listdir(logpath):
        if os.stat(os.path.join(logpath,f)).st_mtime < time.time() - 7 * 86400:
            os.remove(os.path.join(logpath,f))

#Write error logfile
def write_logfile(data, logfilename):
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    with open(os.path.join(logpath,logfilename), 'w') as logfile:
        logfile.write(data)

#Write status to json status file
def write_to_json(id, backupname, starttime, endtime, duration, size, status, info):
    if not os.path.exists(statusJsonFile):
        os.mknod(statusJsonFile)
    with open(statusJsonFile, "r") as jsonFile:
        try:
            data = json.load(jsonFile)
        except JSONDecodeError:
            data = {}
    data[id + "_" + backupname] = {
        'id': id,
        'backupname': backupname,
        'starttime': starttime,
        'endtime': endtime,
        'duration': duration,
        'size': size,
        'status': status,
        'info': info
    }
    with open(statusJsonFile, "w") as jsonFile:
        json.dump(data, jsonFile)

#Read status from json status file. Either in fancy, human friendly manner (plain=False), or for automated reports, in plain text
def read_from_json(plain):
    if not os.path.exists(statusJsonFile):
        os.mknod(statusJsonFile)
    with open(statusJsonFile, "r") as jsonFile:
        try:
            readdata = json.load(jsonFile)
            readdataString = json.dumps(readdata, sort_keys=True)
            readdata = json.loads(readdataString)
            lines = []
            headers=["VM/CT-ID", "Backupname", "Starttime", "Endtime", "Duration", "Size", "Status", "Additional Info"]
            empty_line = []
            for header in headers:
                empty_line.append("")
            # Sort by Backupname
            sorted = {}
            for name, data in readdata.items():
                if not data['backupname'] in sorted:
                    sorted[data['backupname']] = {}
                sorted[data['backupname']][name] = data

            if plain:
                lines = []
                for name, data in sorted.items():
                    for name, data in data.items():
                        line = []
                        line.append(data['id'])
                        line.append(data['backupname'])
                        line.append(data['starttime'])
                        line.append(data['endtime'])
                        line.append(data['duration'])
                        if (data.get('size') is not None):
                            line.append(data['size'])
                        else:
                            line.append("")
                        line.append(data['status'])
                        line.append(data['info'])
                        lines.append(line)
                    lines.append(empty_line)
                format_row = "{:<10} {:<22} {:<21} {:<21} {:<16} {:<8} {:<8} {:<30}"
                print (format_row.format(*headers))
                lines.pop() # remove last item - empty line

                for line in lines:
                    print(format_row.format(*line))

            else:
                for i in range(len(headers)):
                    headers[i] = bcolors.HEADER + headers[i] + bcolors.ENDC

                table = PrettyTable(headers)

                for name, data in sorted.items():
                    for name, data in data.items():
                        table.add_row([(bcolors.BOLD if data['id'] == "all" else "") + data['id'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['backupname'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['starttime'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['endtime'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['duration'] + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['size'] if data.get('size') is not None else "-" + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + (bcolors.FAIL if data['status'] == "error" else bcolors.OKGREEN) + data['status'] + bcolors.ENDC + (bcolors.ENDC if data['id'] == "all" else ""),
                                       (bcolors.BOLD if data['id'] == "all" else "") + data['info'] + (bcolors.ENDC if data['id'] == "all" else "")
                                     ])
                    table.add_row(empty_line)
                row_count = 0
                for row in table:
                    row_count += 1
                table.del_row(row_count -1 )
                print (table)
        except JSONDecodeError:
            return

#Main method for the backup function
def backup(hostname,zfspool,backupname,ids,replicate,raw,properties,maxsnap,retries,prepend_storage_id,dest_config_path=None):
    global debug
    global test
    if replicate:
        replicationtext = " with replication"
    else:
        replicationtext = " without replication"

    if dest_config_path is not None:
        dest_config_path_text = " Config-Path: " + dest_config_path
    else:
        dest_config_path_text = " Config-Path: Default"

    log ("Backing up to " + hostname + ":" + zfspool + "@" + backupname + replicationtext + "," + dest_config_path_text)

    if maxsnap is None:
        maxsnap = 1

    timeformat = "%d-%m-%Y_%H:%M:%S"
    response = ""
    failedOnce = False
    firststarttime = datetime.datetime.now()
    is_pull = False
    destination = zfspool
    if not ("localhost" in hostname or "127.0.0.1" in hostname):
        destination = hostname + ":" + destination


    for id in ids:
        log ("ID " + id + " syncing...")
        if ':' in id:
            is_pull = True
        starttime = datetime.datetime.now()
        if not test:
            write_to_json(id, backupname, starttime.strftime(timeformat), "-", "-", "-", "syncing", "")
        command = ['pve-zsync', 'sync',
                      '--source', id,
                      '--dest', destination,
                      '--name', backupname,
                      '--maxsnap', str(maxsnap),
                      '--method', 'ssh',
                      '--source-user', 'root',
                      '--dest-user', 'root',
                      '--verbose']
        if dest_config_path is not None:
            command.append('--dest-config-path')
            command.append(dest_config_path)
        if replicate:
            command.append('--replicate')
        if raw:
            command.append('--raw')
        if properties:
            command.append('--properties')
        if prepend_storage_id:
            command.append('--prepend-storage-id')

        rc, stdout, stderr, pid = execute_command(command)
        tries = 0

        logfilestrings = ""
        if retries is not None:
            while retries > tries and rc != 0:
                if "include no disk on zfs" in stderr:
                    break #break the retry loop cause "include no disk on zfs" is not an error... just skip this vm/ct id instead
                write_logfile(stderr, str(pid) + '.err')
                logfilestrings = str(pid) + '.err' + " "
                tries+=1
                log ("Failed, will retry after 30 seconds...")
                time.sleep(30)
                log ("Sanitizing remote side...")
                innerArgs = type('innerArgs', (object,),
                 {'hostname':hostname, 'backupname': backupname, 'ids': id, 'zfspool':zfspool})()
                sanitize(innerArgs)

                log ("Retrying backup...")
                rc, stdout, stderr, pid = execute_command(command)

        endtime = datetime.datetime.now()
        duration = endtime - starttime

        if rc != 0:
            if "include no disk on zfs" in stderr:
                if not test:
                    cleanup_json(id)
                    continue #"include no disk on zfs" is not an error... just skip this vm/ct id and continue with the next. We don't need log data either
            log (stderr)
            log ("Command: \"" + ' '.join(command) + "\" failed " + str(tries+1) + " times, no retries left")
            log ("ID " + id + " failed. Took " + str(duration))
            failedOnce = True
            response = response + "ID " + id + " - ERROR - Took " + str(duration) +"\n"
            write_logfile(stderr, str(pid) + '.err')
            if not test:
                write_to_json(id, backupname, starttime.strftime(timeformat), endtime.strftime(timeformat), str(duration), "error", "-" ,"Errorlog at " + os.path.join(logpath,str(pid) + ".err"))
        else:
            log ("ID " + id + " done successfully with " + str (tries+1) + " attempts. Took " + str(duration))
            response = response + "ID " + id + " - OK! - Took " + str(duration) + "\n"
            additionalMessage = ""
            if tries > 0:
                additionalMessage = "Needed " + str(tries) + " additional retries, check " + os.path.join(logpath) + "[" + logfilestrings + "]"
            if not test:
                estimated_total_size_matches = re.findall(r"total estimated size is.*", stderr)
                estimated_size = ""
                if len(estimated_total_size_matches) > 0:
                    for estimated_total_size_match in estimated_total_size_matches:
                       estimated_size_carved_match = re.search(r'(\d+(\.\d+)?(B|K|M|G|T))', estimated_total_size_match)
                       if estimated_size_carved_match is not None:
                           estimated_size = estimated_size + estimated_size_carved_match.group() + ","
                    log_debug ("Sent size: " + str(estimated_size[:-1]))
                    estimated_size = estimated_size[:-1] #Remove trailing ","
                else:
                    estimated_size = "-"
                write_to_json(id, backupname, starttime.strftime(timeformat), endtime.strftime(timeformat), str(duration), estimated_size, "ok", additionalMessage)

    finaltime = datetime.datetime.now()
    finalduration = duration = finaltime - firststarttime
    if not is_pull:
        if not test:
            write_to_json("all", backupname, firststarttime.strftime(timeformat), finaltime.strftime(timeformat), str(finalduration), "-", "error" if failedOnce else "ok", "")

    response = response + "\n" + "Finished in " + str(finalduration)

    ####### PUSH Notification
    return response

#Preperation function to gather all data and make all checks, then execute Backup(...)
def sync(args):
    check_zfs_pool(args.hostname,args.zfspool)

    all_ids = False
    exclude_ids = []
    include_ids = []
    id_list = args.ids.split(',')
    if "all" in id_list:
        id_list.pop(id_list.index("all"))
        all_ids = True
        for id in id_list:
            if id.startswith('-'):
                exclude_ids.append(id.replace("-", ""))
            else:
                print ("Do not use \"all\" in combination with other, non excluding ids!")
                sys.exit(2)
    else:
        for id in id_list:
            if id.startswith('-'):
                print ("Do not use excluding IDs without \"all\"!")
                sys.exit(2)
            else:
                include_ids.append(id)

    vmids = get_ids("qm",include_ids,exclude_ids)
    ctids = get_ids("pct",include_ids,exclude_ids)

    backup_ids = list(dict.fromkeys(vmids + ctids))

    log_debug ("IDs to Backup: " + str(backup_ids))
    log_debug ("Count: " + str(len(backup_ids)))

    if len(backup_ids) > 0:
        lock(args.hostname)
        cleanup_logfolder()
        response = backup(args.hostname, args.zfspool, args.backupname, backup_ids, args.replicate, args.raw, args.properties, args.maxsnap, args.retries, args.prepend_storage_id, args.dest_config_path)
        cleanup_json()
        unlock(args.hostname)
        log ("Backup/Sync finished")


        #execute_command(['/scripts/Notifications/pushnotification', '[PVE-ZSYNC][' + args.backupname + ']', response])

#Disc class for the restore function.
#Each disk has a Name/ID, latest snaoshot, destination (aka pool) and a vm/ct config file
class Disk:
    def parse_id(self):
        id = self.name.split('-')[1]
        return id

    def get_last_snapshot(self, hostname, backupname):
        rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', self.full_name])
        if (rc != 0):
            log ("(SSH) ZFS command error: " + stderr)
            sys.exit(1)
        stdout = stdout.split('\n')
        for x in set(stdout).intersection(considered_empty):
            stdout.remove(x)
        last = [element for element in stdout if(backupname in element)]

        if len(last) > 0:
            return last[-1]
        else:
            log_debug (f"No Snapshot found for { self.name } with backupname { backupname } - skipping...")
            self.skip = True #Backupname not found, skip in favor for others
            return None

    def get_last_config(self, hostname, configs_path):
        rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'ls', '-l', configs_path])
        if (rc != 0):
            log ("(SSH) ls -l command error: " + stderr)
            sys.exit(1)
        stdout = stdout.split('\n')
        for x in set(stdout).intersection(considered_empty):
            stdout.remove(x)
        relevant_files = [element for element in stdout if(self.last_snapshot.split('@')[1] in element and self.id in element)]
        if len(relevant_files) > 0:
            last_config = relevant_files[-1].split(' ')[-1]
            if "qemu" in last_config:
                self.type = "qemu"
            if "lxc" in last_config:
                self.type = "lxc"
            return last_config
        else:
            log_debug (f"No Config file found for {self.full_name}")
            self.skip = True #No Config File for this disk found, skip
            return None

    def get_destination(self, hostname, configs_path):
        rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'cat', configs_path + '/' + self.last_config])
        if (rc != 0):
            log ("(SSH) Get config path command error: " + stderr)
            sys.exit(1)
        stdout = stdout.split('\n\n')[0] #Read only first block of Configfile
        stdout = stdout.split('\n')
        for x in set(stdout).intersection(considered_empty):
            stdout.remove(x)
        diskconfig = [element for element in stdout if (self.name in element)]
        disk = ""
        if len(diskconfig) == 1:
            disk = diskconfig[0].split(',')[0]
            disk = disk.split(':',1)[1].replace(' ','')
        elif len(diskconfig) > 1: #Must have used the new prepent-dataset-id flag of pve-zsync, as pve-zsync would not work in that case
            #we get the destination pool from full_names pre last dataset name which is the pve-storage id if it was sent with prepent-dataset-id
            disk = self.full_name.split('/')[-2] + ':'+ self.name

        rc, stdout, stderr = execute_readonly_command(['pvesm', 'path', disk])
        if (rc != 0):
            log ("pvesm command error: " + stderr)
            sys.exit(1)
        destination = stdout.split('\n')
        for x in set(destination).intersection(considered_empty):
            destination.remove(x)

        if self.type == 'lxc':
            destination = destination[0].split('/',1)[1]
        elif  self.type == 'qemu':
            destination = destination[0].split('/dev/zvol/',1)[1]
        else:
            destination = ""
        return destination

    def __init__(self, hostname, full_name, backupname, configs_path):
        self.restore = False
        self.rollback = False
        self.keep = False
        self.skip = False
        self.full_name = full_name
        self.name = full_name.split('/')[-1]
        self.id = self.parse_id()
        self.last_snapshot = self.get_last_snapshot(hostname, backupname)
        if self.skip: # Can be set in get_last_snapshot
            return
        self.last_config = self.get_last_config(hostname, configs_path)
        if self.skip: # Can be set in get_last_config
            return
        self.destination = self.get_destination(hostname, configs_path)


#Each CT/VM can have multiple disks. A disc group represents all disks of a VM/CT
class Disk_Group:
    def __init__(self, id, type, last_config):
        self.skip = False
        self.id = id
        self.disks = []
        self.type = type
        self.last_config = last_config

    def __eq__(self,other):
        if not isinstance(other, Disk_Group):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.id == other.id


#Parses all zfs disks on the remote side (with an optional filter), and asks the user what should be done to each individual disk.
def gather_restore_data(args):
    global debug
    zfs_disks = check_zfs_pool(args.hostname, args.zfs_source_pool).split('\n')
    zfs_disks = [element for element in zfs_disks if re.search('(subvol|vm)-\d+-disk-\d+', element)]

    if args.filter is not None:
        zfs_disks = [element for element in zfs_disks if args.filter in element]
    for x in set(zfs_disks).intersection(considered_empty):
        zfs_disks.remove(x)
    zfs_disk_objects = []

    if debug:
        print ("Disks found after filter: " + str(zfs_disks))

    for zfs_disk in zfs_disks:
        if args.zfs_source_pool + '/' in zfs_disk:
            zfs_disk_objects.append(Disk(args.hostname, zfs_disk, args.backupname, args.config_path))
            if zfs_disk_objects[-1].skip:
                zfs_disk_objects.pop()
    disk_groups = []
    for disk in zfs_disk_objects:
        if not Disk_Group(disk.id, None, None) in disk_groups:
            group = Disk_Group(disk.id, disk.type, disk.last_config)
            group.disks.append(disk)
            disk_groups.append(group)
        else:
            disk_groups[disk_groups.index(Disk_Group(disk.id, None, None))].disks.append(disk)


    for group in disk_groups:
        print ("ID: " + group.id)
        for disk in group.disks:
            input_data = input ("Restore Disk from " + disk.last_snapshot + " to " + disk.destination + "? (y/n): ")
            while not (input_data == 'y' or input_data == 'n'):
                input_data = input ("Please answer y/n: ")
            if input_data == 'y':
               disk.restore = True
        no_restore_disks = [element for element in group.disks if (element.restore == False)]
        restore_disks = [element for element in group.disks if (element.restore == True)]
        if len(group.disks) > len(restore_disks):
            if len(restore_disks) > 0:
                for no_restore_disk in no_restore_disks:
                    input_data = input ("Fate of " + no_restore_disk.full_name + " - Rollback to same timestamp or keep current data and destroy all newer snapshots? (rollback/keep): ")
                    while not (input_data == 'rollback' or input_data == 'keep'):
                        input_data = input ("Please answer rollback/keep: ")
                    if input_data == 'rollback':
                        no_restore_disk.rollback = True
                    elif input_data == 'keep':
                        no_restore_disk.keep = True
            else:
                group.skip = True
        if len(restore_disks) == 0:
            group.skip = True
    print ("\n\nPlease check restore configuration:")
    for group in disk_groups:
        if group.skip:
            print ("ID: " + group.id + " skipped!")
            continue
        print ("ID: " + group.id + ":")
        for disk in group.disks:
            if disk.restore:
                print ("RESTORE: " +  disk.name + " from " + disk.last_snapshot + " to " + disk.destination + ": ")
            elif disk.rollback:
                print ("ROLLBACK: " + disk.name + " to " + disk.destination + disk.last_snapshot.split('@')[1])
            elif disk.keep:
                print ("KEEP DATA: " + disk.destination)
    input_data = input ("\nIs the information correct? (y):")
    if input_data == 'y':
       return disk_groups
    else:
       return None

#Destroys newer snapshots than the given one. Needed if the data has to be kept, but the snapshots have to be synchronized
def destroy_newer_snapshots(args, destination, snapshot):
    rc, stdout, stderr = execute_readonly_command(['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', destination])
    stdout = stdout.split('\n')
    index_of_snap = stdout.index(destination + '@' + snapshot.split('@')[1])
    snaps_to_delete = stdout[index_of_snap+1:]
    for x in set(snaps_to_delete).intersection(considered_empty):
        snaps_to_delete.remove(x)
    for snap in snaps_to_delete:
        if snap == "":
            continue
        execute_command(['zfs', 'destroy', snap])

#Checks if a dataset is encrypted
def zfs_is_encrypted(dataset):
    rc, stdout, stderr = execute_readonly_command(['zfs', 'get', 'encryption', '-H', '-o', 'value', dataset])
    if rc != 0:
        return False
    elif "off" in stdout:
        return False
    else:
        return True

#Main method for the restore function. Will restore a backup made with pve-zsync-manager or pve-zsync according to the given input in gather_restore_data
def restore(args, disk_groups):
    for group in disk_groups:
        if group.skip:
            print ("VM/CT ID " + group.id + " skipped...")
            continue
        print ("VM/CT ID " + group.id + " preparing...")
        if (group.type == "lxc"):
            execute_command(['pct', 'shutdown', group.id])
            execute_command(['pct', 'set', group.id, '--lock=backup'])

            rc, stdout, stderr, pid = execute_command(['mv', '/etc/pve/lxc/' + group.id + '.conf', '/etc/pve/lxc/' + group.id + '.conf.backup'])
            #if rc != 0:
            #    print (stdout)
            #    print (stderr)
            #    continue

            rc, stdout, stderr, pid = execute_command(['scp', '-B', 'root@' + args.hostname + ':' + args.config_path + '/' + group.last_config, '/etc/pve/lxc/' + group.id + '.conf'])
            if rc != 0:
                print (stdout)
                print (stderr)
                execute_command(['mv', '/etc/pve/lxc/' + group.id + '.conf.backup', '/etc/pve/lxc/' + group.id + '.conf'])
                continue

        elif (group.type == "qemu"):
            execute_command(['qm', 'shutdown', group.id])
            execute_command(['qm', 'set', group.id, '--lock=backup'])

            rc, stdout, stderr, pid = execute_command(['mv', '/etc/pve/qemu-server/' + group.id + '.conf', '/etc/pve/qemu-server/' + group.id + '.conf.backup'])
            if rc != 0:
                print (stdout)
                print (stderr)
                continue

            rc, stdout, stderr, pid = execute_command(['scp', '-B', 'root@' + args.hostname + ':' + args.config_path + '/' + group.last_config, '/etc/pve/qemu-server/' + group.id + '.conf'])
            if rc != 0:
                print (stdout)
                print (stderr)
                execute_command(['mv', '/etc/pve/qemu-server/' + group.id + '.conf.backup', '/etc/pve/qemu-server/' + group.id + '.conf'])
                continue
        no_restore_count = 0

        for disk in group.disks:
            if disk.restore:
                print ("VM/CT ID " + group.id + " - restoring " + disk.destination)
                rc, stdout, stderr = execute_readonly_command(['zfs', 'list', disk.destination])
                if rc == 0:
                    rc, stdout, stderr, pid = execute_command(['zfs', 'destroy', '-r', disk.destination])
                    if rc != 0:
                        print (stdout)
                        print (stderr)
                        continue
                rc, stdout, stderr, pid = execute_command(['ssh -o \"BatchMode yes\" root@' + args.hostname + ' zfs send -Rw ' +  disk.last_snapshot + ' | zfs recv -F ' + disk.destination], shell=True)
                if stderr != "":
                    print (stdout)
                    print (stderr)
                    continue

                if args.keyfile is not None:
                    dataset_encrypted = zfs_is_encrypted(disk.destination)
                    parent_encrypted = zfs_is_encrypted(disk.destination.rsplit('/',1)[0])
                    if dataset_encrypted:
                        rc, stdout, stderr, pid = execute_command(['zfs', 'set', 'keylocation=file://' + args.keyfile, disk.destination])
                        if rc != 0:
                            print (stdout)
                            print (stderr)
                            continue
                        rc, stdout, stderr, pid = execute_command(['zfs', 'load-key', disk.destination])
                        if rc != 0:
                            print (stdout)
                            print (stderr)
                            continue
                    if parent_encrypted:
                        rc, stdout, stderr, pid = execute_command(['zfs', 'change-key', '-i', disk.destination])
                        if rc != 0:
                            print (stdout)
                            print (stderr)
                            continue

                rc, stdout, stderr, pid = execute_command(['zfs', 'mount', disk.destination])
                if rc != 0:
                    print (stdout)
                    print (stderr)
                    continue
            elif disk.rollback:
                no_restore_count = no_restore_count + 1
                print ("VM/CT ID " + group.id + " - rolling back " + disk.destination + " to " + disk.last_snapshot.split('@')[1])
                rc, stdout, stderr, pid = execute_command(['zfs', 'rollback', '-r', disk.destination + disk.last_snapshot.split('@')[1]])
                if rc != 0:
                    print (stdout)
                    print (stderr)
                    continue

            elif disk.keep:
                no_restore_count = no_restore_count + 1
                print ("VM/CT ID " + group.id + " - destroying newer snapshots than " + disk.last_snapshot.split('@')[1] + " on " + disk.destination)
                destroy_newer_snapshots(args, disk.destination, disk.last_snapshot)


        if group.type == "lxc":
            execute_command(['pct', 'unlock', group.id])
        elif group.type == "qemu":
            execute_command(['qm', 'unlock', group.id])

        ## Force Delete PVE Snapshots which are not on all disks
        if no_restore_count > 0:
            cleanup_disks = [ element for element in group.disks if not ( element.restore )]
            if group.type == "lxc":
                snaps_in_config = execute_readonly_command(['pct', 'listsnapshot', group.id])[1]
            elif group.type == "qemu":
                snaps_in_config = execute_readonly_command(['qm', 'listsnapshot', group.id])[1]
            snaps_in_config = snaps_in_config.split('\n')

            for x in set(snaps_in_config).intersection(considered_empty):
                snaps_in_config.remove(x)

            snapnames_in_config = []
            for snap_in_config in snaps_in_config:
                snapnames_in_config.append(snap_in_config.lstrip().split(' ')[1])
                #print ("Snapname: " + snap_in_config.lstrip().split(' ')[1])
            if "current" in snapnames_in_config:
                snapnames_in_config.pop(snapnames_in_config.index("current"))

            all_snaps_on_disks = []
            for disk in cleanup_disks:
                rc, stdout, stderr = execute_readonly_command(['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', disk.destination])
                snapshots_on_disk = stdout.split('\n')
                for x in set(snapshots_on_disk).intersection(considered_empty):
                    snapshots_on_disk.remove(x)
                for snapshot_on_disk in snapshots_on_disk:
                    if not snapshot_on_disk.split('@')[1] in all_snaps_on_disks:
                        #print ("Snap on Disk: " + snapshot_on_disk.split('@')[1])
                        all_snaps_on_disks.append(snapshot_on_disk.split('@')[1])

            for snapname_in_config in snapnames_in_config:
                if not snapname_in_config in all_snaps_on_disks:
                    if group.type == "lxc":
                        print ("Deleting Snapshot " + snapname_in_config + " because it's not present on all disks")
                        execute_command(['pct', 'delsnapshot', group.id, snapname_in_config, '--force'])
                    elif group.type == "qemu":
                        print ("Deleting Snapshot " + snapname_in_config + " because it's not present on all disks")
                        execute_command(['qm', 'delsnapshot', group.id, snapname_in_config, '--force'])

        print ("VM/CT ID " + group.id + " finished!")

#get the lastest snapshot of dataset, or zvol
def get_latest_snapshot(dataset_name, backupname):
    rc, stdout, stderr = execute_readonly_command(['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', dataset_name, '-S', 'creation'])
    stdout = stdout.split('\n')
    for x in set(stdout).intersection(considered_empty):
        stdout.remove(x)
    snaps = [element for element in stdout if(backupname in element)]
    if len(snaps) > 0:
        last = snaps[0]
        return last
    return None

#get CT/VM configdata from dataset
def parse_dataset(type, id):
    if type == 'lxc':
        rc, stdout, stderr = execute_readonly_command(['pct', 'config', id])
    elif type == 'qemu':
        rc, stdout, stderr = execute_readonly_command(['qm', 'config', id])
    stdout = stdout.split('\n')
    for x in set(stdout).intersection(considered_empty):
        stdout.remove(x)
    diskconfigs = [element for element in stdout if (id + '-disk' in element)]
    datasets = []
    for diskconfig in diskconfigs:
        disk = diskconfig.split(',')[0]
        disk = disk.split(':',1)[1].replace(' ','')

        rc, stdout, stderr = execute_readonly_command(['pvesm', 'path', disk])
        dataset = stdout.split('\n')
        for x in set(dataset).intersection(considered_empty):
            dataset.remove(x)

        if type == 'lxc':
            dataset = dataset[0].split('/',1)[1]
        elif  type == 'qemu':
            dataset = dataset[0].split('/dev/zvol/',1)[1]
        else:
            dataset = ""
        datasets.append(str(disk.split(':')[0] + ":" + dataset))
    return datasets


#Main method for sanitzing (aka synchronizing) the local snapshot with the remote snapshot.
#This is usually needed if a backup fails, and will be executed autmatically, if the retries parameter is non 0
def sanitize(args):
    all_ids = False
    exclude_ids = []
    include_ids = []
    id_list = args.ids.split(',')
    if "all" in id_list:
        id_list.pop(id_list.index("all"))
        all_ids = True
        for id in id_list:
            if id.startswith('-'):
                exclude_ids.append(id.replace("-", ""))
            else:
                print ("Do not use \"all\" in combination with other, non excluding ids!")
                sys.exit(2)
    else:
        for id in id_list:
            if id.startswith('-'):
                print ("Do not use excluding IDs without \"all\"!")
                sys.exit(2)
            else:
                include_ids.append(id)

    vmids = get_ids("qm",include_ids,exclude_ids)
    ctids = get_ids("pct",include_ids,exclude_ids)

    disks = []

    for id in vmids:
        disks = disks + parse_dataset("qemu", id)
    for id in ctids:
        disks = disks + parse_dataset("lxc", id)

    log_debug (disks)
    log_debug ("Count: " + str(len(disks)))

    for disk in disks:
        latest_snap = get_latest_snapshot(disk.split(':')[1], args.backupname)
        if latest_snap is not None:
            rollback_to = args.zfspool + '/' + latest_snap.split('/')[-1]
            rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + args.hostname, 'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', rollback_to.split('@')[0]])
            #No need to check for return code, as it will be skipped in case of error
            if stdout != "":
                stdout = stdout.split('\n')
                for x in set(stdout).intersection(considered_empty):
                    stdout.remove(x)
                if rollback_to in stdout:
                    if stdout.index(rollback_to) < len(stdout)-1:
                        rc, stdout, stderr, pid = execute_command(['ssh', '-o', 'BatchMode yes', 'root@' + args.hostname, 'zfs', 'rollback', '-r', rollback_to])
                        if stdout != "" or stderr != "":
                            log (stdout)
                            log (stderr)
            else: #Add pve-zsync 2.1-1 function "prepend-storage-id" - if it can't find a backup with <zfs-destination-pool>/disk it tries with <zfs-destination-pool>/<pve-storage-id>/disk
                rollback_to = args.zfspool + '/' + disk.split(':')[0]  + '/' + latest_snap.split('/')[-1] #prepend-storage-id adds the pve storage id between the destination pool and the dataset"

                #The rest is the same as without prepend-storage-id
                rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + args.hostname, 'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', rollback_to.split('@')[0]])
                #No need to check for return code, as it will be skipped in case of error
                if stdout != "":
                    stdout = stdout.split('\n')
                    for x in set(stdout).intersection(considered_empty):
                        stdout.remove(x)
                    if rollback_to in stdout:
                        if stdout.index(rollback_to) < len(stdout)-1:
                            rc, stdout, stderr, pid = execute_command(['ssh', '-o', 'BatchMode yes', 'root@' + args.hostname, 'zfs', 'rollback', '-r', rollback_to])
                            if stdout != "" or stderr != "":
                                log (stdout)
                                log (stderr)

def main():
    global debug
    global test
    # Command: sync  - Arguments
    syncArgsParser = argparse.ArgumentParser()

    syncArgsRequired = syncArgsParser.add_argument_group('required Arguments')
    syncArgsRequired.add_argument("sync")
    syncArgsRequired.add_argument("--hostname", help="Destination Host for Backups", type=str, required=True)
    syncArgsRequired.add_argument("--zfspool", help="ZFS Destination Pool for Backups", type=str, required=True)
    syncArgsRequired.add_argument("--backupname", help="Name of PVE-ZSYNC Snapshots", type=str, required=True)
    syncArgsRequired.add_argument("--ids", help=" Use VM/CT Numbers, separated with commas, or use \"all\". Exclude with -number e.g --ids all,-1000", type=str, required=True)

    syncArgsParser.add_argument("--dest-config-path", help="Path to store VM/CT config files on destination host", type=str)
    syncArgsParser.add_argument("--replicate", help="Set if Dataset should be replicated with all Snapshots and Properties", action="store_true")
    syncArgsParser.add_argument("--raw", help="Send Dataset in Raw (Encrypted) mode", action="store_true")
    syncArgsParser.add_argument("--maxsnap", help="Keep given amount of snapshots", type=int)
    syncArgsParser.add_argument("--properties", help="Send Dataset with properties (If Dataset is encrypted, raw has to be set too!)", action="store_true")
    syncArgsParser.add_argument("--retries", help="Retry amount of failed backups", type=int)
    syncArgsParser.add_argument("--prepend-storage-id", help="Prepends any VM/CT Disk with it's corresponding pve-storage id (Adds an additinal zfs dataset layer)", action="store_true")
    syncArgsParser.add_argument("--verbose", help="Enable verbose mode", action="store_true")
    syncArgsParser.add_argument("--test", help="Only test the functionality, do not actually execute anything", action="store_true")

    # Command: status - Arguments
    statusArgsParser = argparse.ArgumentParser()

    statusArgsRequired = statusArgsParser.add_argument_group('required Arguments')
    statusArgsRequired.add_argument("status")
    statusArgsParser.add_argument("--verbose", help="Enable verbose mode", action="store_true")
    statusArgsParser.add_argument("--plain", help="Print text without colors", action="store_true")

    # Command: restore - Arguments
    restoreArgsParser = argparse.ArgumentParser()

    restoreArgsRequired = restoreArgsParser.add_argument_group('required Arguments')
    restoreArgsRequired.add_argument("restore")
    restoreArgsRequired.add_argument("--hostname", help="Backup-Source Hostname", type=str, required=True)
    restoreArgsRequired.add_argument("--zfs-source-pool", help="ZFS Source Pool (Same as destination Pool with \"sync\")", type=str, required=True)
    restoreArgsRequired.add_argument("--backupname", help="Name of PVE-ZSYNC Snapshots (Same as with \"sync\")", type=str, required=True)
    restoreArgsRequired.add_argument("--config-path", help="Path to restore VM/CT config files from", type=str, required=True)
    restoreArgsParser.add_argument("--keyfile", help="Path to keyfile, needed for inheriting the ZFS-Key", type=str)
    restoreArgsParser.add_argument("--test", help="Only test the functionality, do not actually execute anything", action="store_true")
    restoreArgsParser.add_argument("--verbose", help="Enable verbose mode", action="store_true")
    restoreArgsParser.add_argument("--filter", help="Filter for given string")

    # Command: sanitize - Arguments
    sanitizeArgsParser = argparse.ArgumentParser()

    sanitizeArgsRequired = sanitizeArgsParser.add_argument_group('required Arguments')
    sanitizeArgsRequired.add_argument("sanitize")
    sanitizeArgsRequired.add_argument("--hostname", help="Host to sanitize", type=str, required=True)
    sanitizeArgsRequired.add_argument("--zfspool", help="ZFS Pool to sanitize", type=str, required=True)
    sanitizeArgsRequired.add_argument("--backupname", help="Name of PVE-ZSYNC Snapshots", type=str, required=True)
    sanitizeArgsRequired.add_argument("--ids", help=" Use VM/CT Numbers, separated with commas, or use \"all\". Exclude with -number e.g --ids all,-1000", type=str, required=True)
    sanitizeArgsParser.add_argument("--verbose", help="Enable verbose mode", action="store_true")
    sanitizeArgsParser.add_argument("--test", help="Only test the functionality, do not actually execute anything", action="store_true")

    if "sync" in sys.argv:
        args = syncArgsParser.parse_args()
        debug = args.verbose
        test = args.test
        if debug:
            log ("Debug mode")
        if test:
            log ("Test mode")
        try:
            log ("Sync started with: " + ' '.join(sys.argv[0:]))
            sync(args)
        except KeyboardInterrupt:
            log ("Interupted by User")
            unlock(args.hostname)
    elif "status" in sys.argv:
        args = statusArgsParser.parse_args()
        read_from_json(args.plain)

    elif "restore" in sys.argv:
        args = restoreArgsParser.parse_args()
        debug = args.verbose
        test = args.test
        if debug:
            print ("Debug mode")
        if test:
            print ("Test mode")
        try:
            disk_groups = gather_restore_data(args)
        except KeyboardInterrupt:
            print ("Interupted by User")
            sys.exit(1)
        if disk_groups is not None:
            try:
                lock(args.hostname)
                restore(args, disk_groups)
                unlock(args.hostname)
            except KeyboardInterrupt:
                print ("Interupted by User")
                unlock(args.hostname)

    elif "sanitize" in sys.argv:
        args = sanitizeArgsParser.parse_args()
        debug = args.verbose
        test = args.test
        if debug:
            print ("Debug mode")
        if test:
            print ("Test mode")
        sanitize(args)
    else:
        print ("ERROR: no command sepcified!")
        print ("")
        print ("USAGE: ")
        print ("    " + sys.argv[0] + " status [OPTIONS]")
        print ("    " + sys.argv[0] + " sync [OPTIONS]")
        print ("    " + sys.argv[0] + " restore [OPTIONS]")
        print ("    " + sys.argv[0] + " sanitize [OPTIONS]")



if __name__ == "__main__":
    main()





