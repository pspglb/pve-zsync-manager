#!/usr/bin/env python3

import os
import re
import logging
import traceback

from Classes.Backed_Up_Disk import Backed_Up_Disk
from Classes.Disk_Group import Disk_Group
from Classes.Snapshot import Snapshot


import pzm_common
from pzm_common import execute_readonly_command, execute_command, check_zfs_pool, log, log_verbose, log_debug, log_input
from pzm_locking import lock, unlock

#Parses all zfs disks on the remote side (with an optional filter), and asks the user what should be done to each individual disk.
def gather_restore_data(args):
    log ("Gathering data from remote side, please wait...")
    zfs_disks = check_zfs_pool(args.hostname, args.zfs_source_pool).split('\n')
    zfs_disks = [element for element in zfs_disks if re.search('(basevol|subvol|vm)-\d+-disk-\d+', element)]

    if args.filter is not None:
        zfs_disks = [element for element in zfs_disks if args.filter in element]
    for x in set(zfs_disks).intersection(pzm_common.considered_empty):
        zfs_disks.remove(x)

    log_verbose ("Disks found after filter: " + str(zfs_disks))

    zfs_found_disk_objects:list[Backed_Up_Disk] = []
    for zfs_disk in zfs_disks:
        if args.zfs_source_pool + '/' in zfs_disk:
            disk = Backed_Up_Disk(args.hostname, zfs_disk, args.backupname, args.config_path)
            if not disk.skip: #If it was skipped during detection, we don't need to bother anymore
                zfs_found_disk_objects.append(disk)

    disk_groups = []
    for disk in zfs_found_disk_objects:
        if not Disk_Group(disk.id, None) in disk_groups:
            group = Disk_Group(disk.id, disk.type)
            group.backed_up_disks.append(disk)
            disk_groups.append(group)
        else:
            disk_groups[disk_groups.index(Disk_Group(disk.id, None))].backed_up_disks.append(disk)

    print ("") #Doesn't need to be in logfile
    for group in disk_groups:
        #Disks which were not found in the specified backup location, but are defined in the configuration
        group.find_non_backed_up_disks(args.hostname, args.config_path)

        log ("ID: " + group.id)
        for disk in group.backed_up_disks:
            input_data = log_input ("Restore Disk from " + disk.last_snapshot + " to " + disk.destination + "? (y/n): ").lower()
            while not (input_data == 'y' or input_data == 'n'):
                input_data = log_input ("Please answer y/n: ").lower()
            if input_data == 'y':
                disk.restore = True

        print ("") #Doesn't need to be in logfile
        no_restore_disks = [element for element in group.backed_up_disks if (element.restore == False)]
        restore_disks = [element for element in group.backed_up_disks if (element.restore == True)]
        if len(group.backed_up_disks) > len(restore_disks):
            if len(restore_disks) > 0: #Only ask for other disks in a CT, if at minimum one disk has to be restored
                for no_restore_disk in no_restore_disks:
                    #Check if it exists locally or if it's in the config, if not, warn user about it later that the The VM/CT will be restored in a broken state
                    rc, stdout, stderr = execute_readonly_command(['zfs', 'list', no_restore_disk.destination])
                    if rc == 0 : #It it was found, ask for it's fate
                        input_data = log_input ("Fate of " + no_restore_disk.unique_name + " - Rollback to same timestamp or keep current data and destroy all newer snapshots? (rollback/keep): ").lower()
                        while not (input_data == 'rollback' or input_data == 'keep'):
                            input_data = log_input ("Please answer rollback/keep: ").lower()
                        if input_data == 'rollback':
                            no_restore_disk.rollback = True
                        elif input_data == 'keep':
                            no_restore_disk.keep = True
                    else: #If it was not found, check if it actually is defined in the config file
                        rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + args.hostname, 'cat', args.config_path + '/' + group.get_last_config()])
                        if no_restore_disk.unique_name not in stdout: #Disk is not in config file, we can safely skip this disk
                            no_restore_disk.skip = True
            else:
                group.skip = True
        if len(restore_disks) == 0:
            group.skip = True

        if not group.skip:
            for non_backed_up_disk in group.non_backed_up_disks:
                rc, stdout, stderr = execute_readonly_command(['zfs', 'list', non_backed_up_disk.destination])
                if rc != 0: #Only ask if a disk should be recreated, if it doesn't already exist locally
                    input_data = log_input ("Disk " + non_backed_up_disk.unique_name + " was not backed up. Should it be recreated? (y/n): ").lower()
                    while not (input_data == 'y' or input_data == 'n'):
                        input_data = log_input ("Please answer y/n: ").lower()
                    if input_data == 'y':
                        non_backed_up_disk.recreate = True
                else:
                    non_backed_up_disk.skip = True

    print ("\n") #Doesn't need to be in logfile
    log ("Please check restore configuration:")
    for group in disk_groups:
        if group.skip:
            log ("ID: " + group.id + " skipped!")
            continue
        log ("ID: " + group.id + ":")
        for backed_up_disk in group.backed_up_disks:
            if backed_up_disk.restore:
                log ("RESTORE: " +  backed_up_disk.unique_name + " from " + backed_up_disk.last_snapshot + " to " + backed_up_disk.destination + ": ")
            elif backed_up_disk.rollback:
                log ("ROLLBACK: " + backed_up_disk.unique_name + " to " + backed_up_disk.destination + '@' + backed_up_disk.last_snapshot.split('@')[1])
            elif backed_up_disk.keep:
                log ("KEEP DATA: " + backed_up_disk.unique_name)
            elif backed_up_disk.skip:
                log ("SKIP: " + backed_up_disk.unique_name)
            else: #Doesn't exist locally, and should not be restored either, and is also not skipped because it isn't defined in the config
                log (pzm_common.bcolors.WARNING + "WARNING: Disk " + backed_up_disk.unique_name + " does not exist locally and was set to don't restore. The " + ("CT" if group.type == "lxc" else "VM") + " will be restored but the config will most likely be broken!" + pzm_common.bcolors.ENDC, logging.WARN)
        for non_backed_up_disk in group.non_backed_up_disks:
            if non_backed_up_disk.recreate:
                log ("RECREATE: " + non_backed_up_disk.unique_name + " to " + non_backed_up_disk.destination)
            elif not non_backed_up_disk.skip:
                log ("DON'T RECREATE: " + non_backed_up_disk.unique_name)

    print ("") #Doesn't need to be in logfile
    input_data = log_input ("Is the information correct? (y): ").lower()
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
    for x in set(snaps_to_delete).intersection(pzm_common.considered_empty):
        snaps_to_delete.remove(x)
    for snap in snaps_to_delete:
        if snap == "":
            continue
        execute_command(['zfs', 'destroy', snap])

#Checks if a dataset is encrypted
def zfs_is_encrypted(dataset, hostname=None):
    command = []
    if hostname is not None:
        command = ['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'zfs', 'get', 'encryption', '-H', '-o', 'value', dataset]
    else:
        command = ['zfs', 'get', 'encryption', '-H', '-o', 'value', dataset]
    rc, stdout, stderr = execute_readonly_command(command)
    if rc != 0:
        return False
    elif "off" in stdout:
        return False
    else:
        return True

### Does the restore of a group based on the previously gathered user input data
def restore_group(group, vm_ct_interaction_command, args):
    config_data = ""
    #Only read data once
    if not args.replicate and group.type == "lxc": #### If the dataset is not replicated, we need the size info from the config. Only on LXC
        rc, stdout, stderr = execute_readonly_command([vm_ct_interaction_command, 'config', group.id])
        if rc == 0:
            config_data = stdout

    ###### Start the restore progress
    for disk in group.backed_up_disks:
        ### If the disk is set to be restored as a whole from backup
        if disk.restore:
            log ("VM/CT ID " + group.id + " - restoring " + disk.destination)
            rc, stdout, stderr = execute_readonly_command(['zfs', 'list', disk.destination])
            if rc == 0:
                rc, stdout, stderr, pid = execute_command(['zfs', 'destroy', '-r', disk.destination])
                if rc != 0:
                    log (stdout, logging.ERROR)
                    log (stderr, logging.ERROR)
                    continue
            send_flags = ""
            if args.replicate:
                flags += "-R"
                if zfs_is_encrypted(disk.full_name, args.hostname):
                    flags += "w" #Raw send needed when replicating an encrypted dataset

            rc, stdout, stderr, pid = execute_command(['ssh -o \"BatchMode yes\" root@' + args.hostname + ' zfs send ' + send_flags + ' ' +  disk.last_snapshot + ' | zfs recv -F ' + disk.destination], shell=True)
            if stderr != "":
                log (stdout, logging.ERROR)
                log (stderr, logging.ERROR)
                continue

            ### If a keyfile was specified, and the disk is/was encrypted load the key in
            if args.keyfile is not None and args.replicate: #Only load key-file if disk is encrypted and sent with replicate - which would need a raw flag
                dataset_encrypted = zfs_is_encrypted(disk.full_name, args.hostname)
                if dataset_encrypted:
                    rc, stdout, stderr, pid = execute_command(['zfs', 'set', 'keylocation=file://' + args.keyfile, disk.destination])
                    if rc != 0:
                        log (stdout, logging.ERROR)
                        log (stderr, logging.ERROR)
                        continue
                    rc, stdout, stderr, pid = execute_command(['zfs', 'load-key', disk.destination])
                    if rc != 0:
                        log (stdout, logging.ERROR)
                        log (stderr, logging.ERROR)
                        continue
                    ### If the parent zfs-dataset of the disk is also encrypted, inherit the key from it
                    parent_encrypted = zfs_is_encrypted(disk.destination.rsplit('/',1)[0])
                    if parent_encrypted:
                        rc, stdout, stderr, pid = execute_command(['zfs', 'change-key', '-i', disk.destination])
                        if rc != 0:
                            log (stdout, logging.ERROR)
                            log (stderr, logging.ERROR)
                            continue

            if group.type == "lxc": #Only attempt to mount a dataset, not a zvol
                rc, stdout, stderr = execute_readonly_command(['zfs', 'get', 'mounted', '-H', '-o', 'value', disk.destination])
                if rc == 0 and "no" in stdout: #If the process errors, there might be something wrong here, if no in stdout then mount the disk
                    rc, stdout, stderr, pid = execute_command(['zfs', 'mount', disk.destination])
                    if rc != 0:
                        log (stdout, logging.ERROR)
                        log (stderr, logging.ERROR)
                        continue
            #Set refquota on lxc containers. Also when replicated as we can't be sure it has refquota set on remote side (non replicated sync to destination, replicated sync back)
            #Don't know why someone would do that, but lets be as safe as possible
            if config_data != "" and group.type == "lxc":
                try:
                    config_line = [x for x in config_data.split('\n') if disk.unique_name in x][0]
                    #config line: "mp0: vmsys:subvol-100-disk-1,mp=/test,backup=1,size=8G"
                    #options: "mp=/test,backup=1,size=8G" as list
                    options = config_line.split(',',1)[1].split(',')
                    #from "size=8G" to "8"
                    size = [element for element in options if 'size=' in element][0].split('=')[1]
                    rc, stdout, stderr, pid = execute_command(['zfs', 'set', 'refquota=' + size, disk.destination])
                    if rc != 0:
                        log ("VM/CT ID " + group.id + " - Could not set refquota: " + stderr, logging.WARN)
                except:
                    log ("VM/CT ID " + group.id + " - Error during config_line parsing: " + traceback.format_exc(), logging.WARN)


        ### Disk which are set to rollback, will just rollback the local disk to the snapshot which has the same timestamp as a restore disk
        elif disk.rollback:
            log ("VM/CT ID " + group.id + " - rolling back " + disk.destination + " to " + disk.last_snapshot.split('@')[1])
            rc, stdout, stderr, pid = execute_command(['zfs', 'rollback', '-r', disk.destination + '@' + disk.last_snapshot.split('@')[1]])
            if rc != 0:
                log (stdout, logging.ERROR)
                log (stderr, logging.ERROR)
                continue

        ### Disk which are set to keep, will not alter any current data, but delete any newer snapshots than the timestamp of the restore disk
        elif disk.keep:
            log ("VM/CT ID " + group.id + " - destroying newer snapshots than " + disk.last_snapshot.split('@')[1] + " on " + disk.destination)
            destroy_newer_snapshots(args, disk.destination, disk.last_snapshot)


def convert_to_gib(value):
    #Input values can be: G, T, P, E etc.
    try: #If conversion error
        if re.match(r'\d+\s*G', value):
            #GiB
            size_in_gib = int(re.sub('[\sG]+', '',value))
            return str(size_in_gib)
        if re.match(r'\d+\s*T', value):
            #TiB
            size_in_tib = int(re.sub('[\sT]+', '',value))
            return str(size_in_tib * 1024)
        if re.match(r'\d+\s*P', value):
            #PiB
            size_in_pib = int(re.sub('[\sP]+', '',value))
            return str(size_in_pib * 1024 * 1024)
        if re.match(r'\d+\s*E', value):
            #EiB
            size_in_eib = int(re.sub('[\sE]+', '',value))
            return str(size_in_eib * 1024 * 1024 * 1024)
    except ValueError:
        log_debug ("Conversion error " + traceback.format_exc())
        return None
    return None



### Recreates disks of a group which were not backed up to the remote side and user defined to do so
def recreate_disks_of_group(group, vm_ct_interaction_command):
    ###### Unlock for recreating #####
    if len(group.non_backed_up_disks) > 0:
        execute_command([vm_ct_interaction_command, 'unlock', group.id])

    ###### Recreate disk if it was not backed up and set to recreate #####
    for non_backed_up_disk in group.non_backed_up_disks:
        if non_backed_up_disk.recreate:
            log ("VM/CT ID " + group.id + " - Recreating " + non_backed_up_disk.unique_name)
            try:
                #config line: "mp0: vmsys:subvol-100-disk-1,mp=/test,backup=1,size=8G"
                #options: "mp=/test,backup=1,size=8G" as list
                options = non_backed_up_disk.config_line.split(',',1)[1].split(',')
                 #hardware_id: mp1, scsi1, sata1 etc
                hardware_id = non_backed_up_disk.config_line.split(':', 1)[0]
                #from "size=8G" to "8"
                size_raw = [element for element in options if 'size=' in element][0].split('=')[1]
                size_converted = convert_to_gib(size_raw)
                if size_converted is None:
                    log ("VM/CT ID " + group.id + " - Can't convert " + size_raw + " to GiB, can't recreate disk", logging.ERROR)
                    continue
                #storage pool:from unique_name "vmsys:subvol-100-disk-1" to "vmsys"
                storage_pool = non_backed_up_disk.unique_name.split(':')[0]
                rc, stdout, stderr, pid = execute_command([vm_ct_interaction_command, 'set', group.id, f'--{hardware_id}', f"{storage_pool}:{size_converted},{','.join(options)}"])
                if rc != 0:
                    log (stdout, logging.ERROR)
                    log (stderr, logging.ERROR)
                    continue
            except Exception:
                log ("VM/CT ID " + group.id + " - Error in recreation " + traceback.format_exc(), logging.ERROR)

    ###### Lock again for next step #####
    if len(group.non_backed_up_disks) > 0:
        execute_command([vm_ct_interaction_command, 'set', group.id, '--lock=backup'])
    
###Checks consistency of snapshots on disk vs snapshots in the restored config
def snapshot_consistency_check(group, config_file_path, vm_ct_interaction_command):
    log ("VM/CT ID " + group.id + " - Checking snapshot consistency, this may take a while.")
    cleanup_disks = group.backed_up_disks + group.non_backed_up_disks

    config = []
    with open(config_file_path, 'r') as config_file:
        config = config_file.readlines()
    config_new = config.copy() #Copy to have to have the reference


    snapnames_in_config = execute_readonly_command([vm_ct_interaction_command, 'listsnapshot', group.id])[1].split('\n')
    snapnames_in_config = [x.lstrip().split(' ')[1] for x in snapnames_in_config if "current" not in x and x not in pzm_common.considered_empty]


    snapshots_in_config = [Snapshot(x) for x in snapnames_in_config]


    ##### Iterate over all snapnames in the config file and search for inconsistencies
    for snapshot_in_config in snapshots_in_config:
        current_config_len = len(config_new)

        rc, stdout, stderr = execute_readonly_command([vm_ct_interaction_command, 'config', group.id, '--snapshot', snapshot_in_config.name])
        snapshot_config = stdout

        for disk in cleanup_disks:
            if disk.unique_name in snapshot_config: #If the disk is mentioned in the snapshot config, we have to check if the snapshot truly exists on the disk
                if snapshot_in_config.name not in disk.get_all_snapshots_on_disk(): #If snapname_in_config is not in the list of snapshots on disk, delete it's reference
                    if not pzm_common.test:
                        log_verbose ("VM/CT ID " + group.id + " - Deleting reference of " + disk.unique_name + " in snapshot " + snapshot_in_config.name)
                        #Delete this snapshot from the config
                        #Read all lines from config, search for the snapshot name, delete the whole line where disk.unique_name is found at the next occourance
                        found_snapshot = False
                        config_tmp = []
                        for line in config_new:
                            if "[" + snapshot_in_config.name + "]" in line and "parent" not in line: #Found snapshot header. Can only occour once in config file
                                found_snapshot = True
                            if found_snapshot and disk.unique_name in line:
                                #the snapshot was found previously and the line matches, skip writing that line, and set found_snapshot to False again
                                #so it doesn't skip further occourance
                                found_snapshot = False
                            else:
                                config_tmp.append(line)
                        config_new = config_tmp
                    else:
                        log_verbose ("VM/CT ID " + group.id + " - Would delete reference of " + disk.unique_name + " in snapshot " + snapshot_in_config.name)
                else:
                    snapshot_in_config.keep_snapshot = True #Snapshot in config has at least one defined disk
                    log_verbose ("VM/CT ID " + group.id + " - Disk " + disk.unique_name + " - snapshot " + snapshot_in_config.name + " is OK!")

        if snapshot_in_config.keep_snapshot: #Only show this line, if the snapshot will be kept
            deleted_lines = current_config_len - len(config_new)
            if deleted_lines == 0:
                log_verbose ("VM/CT ID " + group.id + " - No disk definitions from " + snapshot_in_config.name + " deleted, snapshot is consistent")
            else:
                log_verbose ("VM/CT ID " + group.id + " - Deleted " + str(deleted_lines) + " disks from " + snapshot_in_config.name + " as the snapshot can't be found on disk")

    #Config must have changed, if string list isn't of the same length anymore
    if len(config) != len(config_new):
        if not pzm_common.test:
            log_verbose ("VM/CT ID " + group.id + " - Writing new config file for " + group.id + ", as file has changed by " + str(len(config)-len(config_new)) + " lines.")
            with open(config_file_path, 'w') as config_file:
                config_file.writelines(config_new)
        else:
            log_verbose ("VM/CT ID " + group.id + " - Would write new config file for " + group.id + ", as file has changed by " + str(len(config)-len(config_new)) + " lines.")


    ##### If backup config exists, compare it to the restored config and add those which are not present in the restored config to a "delete snapshot from disk" list
    if (os.path.exists(config_file_path + ".backup")):
        #Needed to delete snapshots from disk which are no more referenced
        old_config = []
        with open(config_file_path + ".backup", 'r') as old_config_file:
            old_config = old_config_file.readlines()

        matches_old_config = re.findall(r"^\[[\w\d\_\-]+\]$", ''.join(old_config), re.MULTILINE) #Find pattern: [autoWeekly_2021-11-28_00-25-02]
        matches_old_config = [x.replace('[', '').replace(']','') for x in matches_old_config] #remove square braces

        matches_restored_config = re.findall(r"^\[[\w\d\_\-]+\]$", ''.join(config_new), re.MULTILINE)
        matches_restored_config = [x.replace('[', '').replace(']','') for x in matches_restored_config]

        #Remove snapshots which are present in both config files from old config matches
        #Old config matches should then only contain snapshot which are no more present on disk
        for x in set(matches_old_config).intersection(matches_restored_config):
            matches_old_config.remove(x)
        snapshots_to_delete_from_disk = matches_old_config

        ###### Delete snapshots from disk which were present in the old config, but not in the restored config
        for disk in cleanup_disks:
            deleted_snaps = 0
            for snapshot in set(snapshots_to_delete_from_disk).intersection(snapnames_in_config):
                rc, stout, stderr, pid = execute_command(['zfs', 'destroy', disk.destination + '@' + snapshot])
                if rc != 0:
                    log (stdout, logging.WARN)
                    log (stderr, logging.WARN)
                else:
                    deleted_snaps = deleted_snaps +1
            if deleted_snaps > 0:
                if pzm_common.test:
                    log_verbose ("Would have deleted " + str(deleted_snaps) + " snapshots from " + disk.unique_name + " as they are not defined in the restored config")
                else:
                    log_verbose ("Deleted " + str(deleted_snaps) + " snapshots from " + disk.unique_name + " as they are not defined in the restored config")
            else:
                log_verbose ("VM/CT ID " + group.id + " - Nothing to delete from " + disk.unique_name + ", snapshots are consistent")


    ##### Important, only delete the snapshot AFTER the config file was re-written, otherwise we would overwrite the deletions with the write command
    snapshots_to_delete_completely = [snap for snap in snapshots_in_config if not snap.keep_snapshot]

    ### Unlock for deleting snapshot
    if len(snapshots_to_delete_completely) > 0:
        execute_command([vm_ct_interaction_command, 'unlock', group.id])

    for snapshot_to_delete in snapshots_to_delete_completely:
        if not pzm_common.test:
            log_verbose ("VM/CT ID " + group.id + " - Deleting snapshot " + snapshot_to_delete.name  + " as it doesn't exist on any disk")
        else:
            log_verbose ("VM/CT ID " + group.id + " - Would delete snapshot " + snapshot_to_delete.name  + " as it doesn't exist on any disk")

        execute_command([vm_ct_interaction_command, 'delsnapshot', group.id, snapshot_to_delete.name])


#Main method for the restore function. Will restore a backup made with pve-zsync-manager or pve-zsync according to the given input in gather_restore_data
def restore(args, disk_groups):
    if not pzm_common.test: lock(args.hostname)
    for group in disk_groups:
        if group.skip:
            log ("VM/CT ID " + group.id + " skipped...")
            continue
        log ("VM/CT ID " + group.id + " preparing...")

        config_file_path = ""
        vm_ct_interaction_command = ""
        if (group.type == "lxc"):
            config_file_path = '/etc/pve/lxc/' + group.id + '.conf'
            vm_ct_interaction_command = 'pct'
        if (group.type == "qemu"):
            config_file_path = '/etc/pve/qemu-server/' + group.id + '.conf'
            vm_ct_interaction_command = 'qm'


        ###### Shutdown VM/CT, lock it so the config won't be altered by PVE, back up the old config if exists, and copy over the backup config
        execute_command([vm_ct_interaction_command, 'shutdown', group.id])
        execute_command([vm_ct_interaction_command, 'set', group.id, '--lock=backup'])

        rc, stdout, stderr, pid = execute_command(['mv', config_file_path, config_file_path + '.backup'])
        #if rc != 0:
        #    log (stdout)
        #    log (stderr)
        #    continue

        rc, stdout, stderr, pid = execute_command(['scp', '-B', 'root@' + args.hostname + ':' + args.config_path + '/' + group.get_last_config(), config_file_path])
        if rc != 0:
            log (stdout, logging.ERROR)
            log (stderr, logging.ERROR)
            execute_command(['mv', config_file_path + '.backup', config_file_path])
            continue
        
        restore_group(group, vm_ct_interaction_command, args)

        recreate_disks_of_group(group, vm_ct_interaction_command)

        #Note: config will be unlocked after snapshot_consistency check. Has to be locked again if additional steps are applied after this function call
        snapshot_consistency_check(group, config_file_path, vm_ct_interaction_command)
        
        log ("VM/CT ID " + group.id + " finished!")
    unlock(args.hostname)
