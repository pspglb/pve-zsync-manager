#!/usr/bin/env python3

import time
import datetime
import os
from json.decoder import JSONDecodeError

import pzm_common
from pzm_common import execute_readonly_command, execute_command, check_zfs_pool, log, log_debug
from pzm_locking import lock, unlock


#Removed CT/VM IDs which no longer exist from the status file.
def cleanup_json(delete = ""):
    if not os.path.exists(pzm_common.statusJsonFile):
        os.mknod(pzm_common.statusJsonFile)
    with open(pzm_common.statusJsonFile, "r") as jsonFile:
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
        with open(pzm_common.statusJsonFile, "w") as jsonFile:
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
    if not os.path.exists(pzm_common.statusJsonFile):
        os.mknod(pzm_common.statusJsonFile)
    with open(pzm_common.statusJsonFile, "r") as jsonFile:
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
    with open(pzm_common.statusJsonFile, "w") as jsonFile:
        json.dump(data, jsonFile)



#Main method for the backup function
def backup(hostname,zfspool,backupname,ids,replicate,raw,properties,maxsnap,retries,prepend_storage_id,dest_config_path=None):
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
        if not pzm_common.test:
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
                if not pzm_common.test:
                    cleanup_json(id)
                    continue #"include no disk on zfs" is not an error... just skip this vm/ct id and continue with the next. We don't need log data either
            log (stderr)
            log ("Command: \"" + ' '.join(command) + "\" failed " + str(tries+1) + " times, no retries left")
            log ("ID " + id + " failed. Took " + str(duration))
            failedOnce = True
            response = response + "ID " + id + " - ERROR - Took " + str(duration) +"\n"
            write_logfile(stderr, str(pid) + '.err')
            if not pzm_common.test:
                write_to_json(id, backupname, starttime.strftime(timeformat), endtime.strftime(timeformat), str(duration), "error", "-" ,"Errorlog at " + os.path.join(logpath,str(pid) + ".err"))
        else:
            log ("ID " + id + " done successfully with " + str (tries+1) + " attempts. Took " + str(duration))
            response = response + "ID " + id + " - OK! - Took " + str(duration) + "\n"
            additionalMessage = ""
            if tries > 0:
                additionalMessage = "Needed " + str(tries) + " additional retries, check " + os.path.join(logpath) + "[" + logfilestrings + "]"
            if not pzm_common.test:
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
        if not pzm_common.test:
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

