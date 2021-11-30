#!/usr/bin/env python3

import time
import datetime
import os
import re
import sys

import pzm_common
from pzm_common import execute_readonly_command, execute_command, check_zfs_pool, log, log_debug
from pzm_locking import lock, unlock


def get_unique_name_from_config_line(config_line):
    #from for example rootfs: vmssd:subvol-100-disk-0,mountpoint=.... to vmssd:subvol-100-disk-0
    return config_line.split(',')[0].split(':',1)[1].replace(' ','')

#Disc class for the restore function.
#Each disk has a Name/ID, latest snaoshot, destination (aka pool) and a vm/ct config file
class Disk:
    def parse_id(self):
        id = self.name.split('-')[1]
        return id

    def get_unique_name(self, hostname, config_file):
        rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'cat', config_file])
        if (rc != 0):
            log ("(SSH) Get config path command error: " + stderr)
            sys.exit(1)
        stdout = stdout.split('\n\n')[0] #Read only first block of Configfile
        stdout = stdout.split('\n')
        for x in set(stdout).intersection(pzm_common.considered_empty):
            stdout.remove(x)
        diskconfig = [element for element in stdout if (self.name in element)]
        disk = ""
        if len(diskconfig) == 1:
            disk = get_unique_name_from_config_line(diskconfig[0])
        elif len(diskconfig) > 1: #Must have used the new prepent-dataset-id flag of pve-zsync, as pve-zsync would not work in that case
            #we get the destination pool from full_names pre last dataset name which is the pve-storage id if it was sent with prepent-dataset-id
            #Example: backuppool/vmsys/subvol-100-disk-0: self.full_name.split('/')[-2] will be "vmsys", self.name subvol-100-disk-0
            disk = self.full_name.split('/')[-2] + ':'+ self.name


        #disk: "<storage_pool>:<disk_identification>" this has to be unique for each disk
        self.unique_name = disk

    def get_destination(self):
        rc, stdout, stderr = execute_readonly_command(['pvesm', 'path', self.unique_name])
        if (rc != 0):
            log ("pvesm command error: " + stderr)
            sys.exit(1)
        destination = stdout.split('\n')
        for x in set(destination).intersection(pzm_common.considered_empty):
            destination.remove(x)

        if self.type == 'lxc':
            destination = destination[0].split('/',1)[1]
        elif  self.type == 'qemu':
            destination = destination[0].split('/dev/zvol/',1)[1]
        else:
            destination = ""
        return destination

    def get_all_snapshots_on_disk(self): #Cachinng method: Parses all snapshots of a disk destination once, then returns the cached result
        if len(self.snapshots_on_disk) > 0: return self.snapshots_on_disk #Was already checked
        rc, stdout, stderr = execute_readonly_command(['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', self.destination])
        snapshots_on_disk = stdout.split('\n')
        for x in set(snapshots_on_disk).intersection(pzm_common.considered_empty):
            snapshots_on_disk.remove(x)

        #We only want the snapshot name
        snapshots_on_disk = [element.split('@')[1] for element in snapshots_on_disk]
        self.snapshots_on_disk = snapshots_on_disk
        return self.snapshots_on_disk

    def __init__(self):
        self.unique_name = ""
        self.skip = False
        self.snapshots_on_disk = []


class Backed_Up_Disk(Disk):
    def get_last_snapshot(self, hostname, backupname):
        rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', self.full_name])
        if (rc != 0):
            log ("(SSH) ZFS command error: " + stderr)
            sys.exit(1)
        stdout = stdout.split('\n')
        for x in set(stdout).intersection(pzm_common.considered_empty):
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
        for x in set(stdout).intersection(pzm_common.considered_empty):
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

    def __init__(self, hostname, full_name, backupname, configs_path):
        super().__init__()
        self.restore = False
        self.rollback = False
        self.keep = False
        self.full_name = full_name
        self.name = full_name.split('/')[-1]
        self.id = self.parse_id()
        self.last_snapshot = self.get_last_snapshot(hostname, backupname)
        if self.skip: # Can be set in get_last_snapshot
            return
        self.last_config = self.get_last_config(hostname, configs_path)
        if self.skip: # Can be set in get_last_config
            return
        self.get_unique_name(hostname, configs_path + '/' + self.last_config)
        self.destination = self.get_destination()

class Non_Backed_Up_Disk(Disk):
    def __init__(self, config_file, config_line, type):
        super().__init__()
        self.unique_name = get_unique_name_from_config_line(config_line)
        self.type = type
        self.config_line = config_line
        self.destination = self.get_destination()
        self.recreate = False


#Each CT/VM can have multiple disks. A disc group represents all disks of a VM/CT
class Disk_Group:
    def __init__(self, id, type):
        self.skip = False
        self.id = id
        self.backed_up_disks:list[Backed_Up_Disk] = []
        self.non_backed_up_disks:list[Non_Backed_Up_Disk] = []
        self.type = type

    def get_last_config(self):
        configs = [disk.last_config for disk in self.backed_up_disks]
        if configs is not None and len(configs) > 0:
            #100.conf.qemu.rep_backup-name_2021-11-27_00:13:50
            configs.sort(key = lambda x: '_'.join([x.split('_')[-2], x.split('_')[-1]]),reverse=True)
            return configs[0]
        else:
            return None

    # Parse additional mountpoints or disks, which are not yet it the disks list
    def find_non_backed_up_disks(self, hostname, configs_path):
        if self.get_last_config() == None:
            return #Without any config, we can't check the remote config....

        rc, stdout, stderr = execute_readonly_command(['ssh', '-o', 'BatchMode yes', 'root@' + hostname, 'cat', configs_path + '/' + self.get_last_config()])
        if (rc != 0):
            log ("(SSH) Get config path command error: " + stderr)
            sys.exit(1)
        stdout = stdout.split('\n\n')[0] #Read only first block of Configfile
        stdout = stdout.split('\n')
        for x in set(stdout).intersection(pzm_common.considered_empty):
            stdout.remove(x)
        all_disks = [element for element in stdout if f"-{self.id}-disk-" in element]


        #Only use disks, which were not found at the backup location
        parsed_non_backed_up_disk_config_lines = [element for element in all_disks if len([backed_up_disk for backed_up_disk in self.backed_up_disks if get_unique_name_from_config_line(element) in backed_up_disk.unique_name]) == 0]


        for config_line in parsed_non_backed_up_disk_config_lines:
            self.non_backed_up_disks.append(Non_Backed_Up_Disk(configs_path + '/' + self.get_last_config(), config_line, self.type))

    def __eq__(self,other):
        if not isinstance(other, Disk_Group):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.id == other.id


class Snapshot:
    def __init__(self, name):
        self.name = name
        self.keep_snapshot = False #Will be used during snapshot consistency check. If a snapshot has disks, it will me marked to be kept


    def __eg__(self, other):
        if not isinstance(other, Snapshot):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.name == other.name


#Parses all zfs disks on the remote side (with an optional filter), and asks the user what should be done to each individual disk.
def gather_restore_data(args):
    print ("Gathering data from remote side, please wait...")
    zfs_disks = check_zfs_pool(args.hostname, args.zfs_source_pool).split('\n')
    zfs_disks = [element for element in zfs_disks if re.search('(basevol|subvol|vm)-\d+-disk-\d+', element)]

    if args.filter is not None:
        zfs_disks = [element for element in zfs_disks if args.filter in element]
    for x in set(zfs_disks).intersection(pzm_common.considered_empty):
        zfs_disks.remove(x)

    if pzm_common.debug:
        print ("Disks found after filter: " + str(zfs_disks))

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

    print ("")
    for group in disk_groups:
        #Disks which were not found in the specified backup location, but are defined in the configuration
        group.find_non_backed_up_disks(args.hostname, args.config_path)

        print ("ID: " + group.id)
        for disk in group.backed_up_disks:
            input_data = input ("Restore Disk from " + disk.last_snapshot + " to " + disk.destination + "? (y/n): ").lower()
            while not (input_data == 'y' or input_data == 'n'):
                input_data = input ("Please answer y/n: ").lower()
            if input_data == 'y':
                disk.restore = True

        print ("")
        no_restore_disks = [element for element in group.backed_up_disks if (element.restore == False)]
        restore_disks = [element for element in group.backed_up_disks if (element.restore == True)]
        if len(group.backed_up_disks) > len(restore_disks):
            if len(restore_disks) > 0: #Only ask for other disks in a CT, if at minimum one disk has to be restored
                for no_restore_disk in no_restore_disks:
                    #Check if it exists locally or if it's in the config, if not, warn user about it later that the The VM/CT will be restored in a broken state
                    rc, stdout, stderr = execute_readonly_command(['zfs', 'list', no_restore_disk.destination])
                    if rc == 0 : #It it was found, ask for it's fate
                        input_data = input ("Fate of " + no_restore_disk.unique_name + " - Rollback to same timestamp or keep current data and destroy all newer snapshots? (rollback/keep): ").lower()
                        while not (input_data == 'rollback' or input_data == 'keep'):
                            input_data = input ("Please answer rollback/keep: ").lower()
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
                    input_data = input ("Disk " + non_backed_up_disk.unique_name + " was not backed up. Should it be recreated? (y/n): ").lower()
                    while not (input_data == 'y' or input_data == 'n'):
                        input_data = input ("Please answer y/n: ").lower()
                    if input_data == 'y':
                        non_backed_up_disk.recreate = True
                else:
                    non_backed_up_disk.skip = True

    print ("\n\nPlease check restore configuration:")
    for group in disk_groups:
        if group.skip:
            print ("ID: " + group.id + " skipped!")
            continue
        print ("ID: " + group.id + ":")
        for backed_up_disk in group.backed_up_disks:
            if backed_up_disk.restore:
                print ("RESTORE: " +  backed_up_disk.unique_name + " from " + backed_up_disk.last_snapshot + " to " + backed_up_disk.destination + ": ")
            elif backed_up_disk.rollback:
                print ("ROLLBACK: " + backed_up_disk.unique_name + " to " + backed_up_disk.destination + '@' + backed_up_disk.last_snapshot.split('@')[1])
            elif backed_up_disk.keep:
                print ("KEEP DATA: " + backed_up_disk.unique_name)
            elif backed_up_disk.skip:
                print ("SKIP: " + backed_up_disk.unique_name)
            else: #Doesn't exist locally, and should not be restored either, and is also not skipped because it isn't defined in the config
                print (pzm_common.bcolors.WARNING + "WARNING: Disk " + backed_up_disk.unique_name + " does not exist locally and was set to don't restore. The " + ("CT" if group.type == "lxc" else "VM") + " will be restored but the config will most likely be broken!" + pzm_common.bcolors.ENDC)
        for non_backed_up_disk in group.non_backed_up_disks:
            if non_backed_up_disk.recreate:
                print ("RECREATE: " + non_backed_up_disk.unique_name + " to " + non_backed_up_disk.destination)
            elif not non_backed_up_disk.skip:
                print ("DON'T RECREATE: " + non_backed_up_disk.unique_name)

    print ("")
    input_data = input ("Is the information correct? (y): ".lower())
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
    if not pzm_common.test: lock(args.hostname)
    for group in disk_groups:
        if group.skip:
            print ("VM/CT ID " + group.id + " skipped...")
            continue
        print ("VM/CT ID " + group.id + " preparing...")

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
        #    print (stdout)
        #    print (stderr)
        #    continue

        rc, stdout, stderr, pid = execute_command(['scp', '-B', 'root@' + args.hostname + ':' + args.config_path + '/' + group.get_last_config(), config_file_path])
        if rc != 0:
            print (stdout)
            print (stderr)
            execute_command(['mv', config_file_path + '.backup', config_file_path])
            continue

        no_restore_count = 0

        ###### Start the restore progress
        for disk in group.backed_up_disks:
            ### If the disk is set to be restored as a whole from backup
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

                ### If a keyfile was specified, and the disk is/was encrypted load the key in
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
                        ### If the parent zfs-dataset of the disk is also encrypted, inherit the key from it
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

            ### Disk which are set to rollback, will just rollback the local disk to the snapshot which has the same timestamp as a restore disk
            elif disk.rollback:
                no_restore_count = no_restore_count + 1
                print ("VM/CT ID " + group.id + " - rolling back " + disk.destination + " to " + disk.last_snapshot.split('@')[1])
                rc, stdout, stderr, pid = execute_command(['zfs', 'rollback', '-r', disk.destination + '@' + disk.last_snapshot.split('@')[1]])
                if rc != 0:
                    print (stdout)
                    print (stderr)
                    continue

            ### Disk which are set to keep, will not alter any current data, but delete any newer snapshots than the timestamp of the restore disk
            elif disk.keep:
                no_restore_count = no_restore_count + 1
                print ("VM/CT ID " + group.id + " - destroying newer snapshots than " + disk.last_snapshot.split('@')[1] + " on " + disk.destination)
                destroy_newer_snapshots(args, disk.destination, disk.last_snapshot)



        ###### Unlock for recreating #####
        if len(group.non_backed_up_disks) > 0:
            execute_command([vm_ct_interaction_command, 'unlock', group.id])

        ###### Recreate disk if it was not backed up and set to recreate #####
        for non_backed_up_disk in group.non_backed_up_disks:
            if non_backed_up_disk.recreate:
                print ("VM/CT ID " + group.id + " - Recreating " + non_backed_up_disk.unique_name)
                #config line: "mp0: vmsys:subvol-100-disk-1,mp=/test,backup=1,size=8G"
                #options: "mp=/test,backup=1,size=8G" as list
                options = non_backed_up_disk.config_line.split(',',1)[1].split(',')
                #hardware_id: mp1, scsi1, sata1 etc
                hardware_id = non_backed_up_disk.config_line.split(':', 1)[0]
                #from "size=8G" to "8"
                size = re.sub('[a-zA-Z]', '', [element for element in options if 'size=' in element][0].split('=')[1])
                #storage pool:from unique_name "vmsys:subvol-100-disk-1" to "vmsys"

                storage_pool = non_backed_up_disk.unique_name.split(':')[0]
                rc, stdout, stderr, pid = execute_command([vm_ct_interaction_command, 'set', group.id, f'--{hardware_id}', f"{storage_pool}:{size},{','.join(options)}"])
                if rc != 0:
                    print (stdout)
                    print (stderr)
                    continue

        ###### Lock again for snapshot cleanup #####
        if len(group.non_backed_up_disks) > 0:
            execute_command([vm_ct_interaction_command, 'set', group.id, '--lock=backup'])


        ############################################################## Snapshot consitency check ####################################################################

        print ("VM/CT ID " + group.id + " - Checking snapshot consistency, this may take a while.")
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
                            if pzm_common.debug: print ("VM/CT ID " + group.id + " - Deleting reference of " + disk.unique_name + " in snapshot " + snapshot_in_config.name)
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
                            if pzm_common.debug: print ("VM/CT ID " + group.id + " - Would delete reference of " + disk.unique_name + " in snapshot " + snapshot_in_config.name)
                    else:
                        snapshot_in_config.keep_snapshot = True #Snapshot in config has at least one defined disk
                        if pzm_common.debug: print ("VM/CT ID " + group.id + " - Disk " + disk.unique_name + " - snapshot " + snapshot_in_config.name + " is OK!")

            if snapshot_in_config.keep_snapshot: #Only show this line, if the snapshot will be kept
                deleted_lines = current_config_len - len(config_new)
                if deleted_lines == 0:
                    print ("VM/CT ID " + group.id + " - No disk definintions from " + snapshot_in_config.name + " deleted, snapshot is consistent")
                else:
                    print ("VM/CT ID " + group.id + " - Deleted " + str(deleted_lines) + " disks from " + snapshot_in_config.name + " as the snapshot can't be found on disk")

        #Config must have changed, if string list isn't of the same length anymore
        if len(config) != len(config_new):
            if not pzm_common.test:
                if pzm_common.debug: print ("VM/CT ID " + group.id + " - Writing new config file for " + group.id + ", as file has changed by " + str(len(config)-len(config_new)) + " lines.")
                with open(config_file_path, 'w') as config_file:
                    config_file.writelines(config_new)
            else:
                if pzm_common.debug: print ("VM/CT ID " + group.id + " - Would write new config file for " + group.id + ", as file has changed by " + str(len(config)-len(config_new)) + " lines.")


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
                    rc, stout, stderr = execute_command(['zfs', 'destroy', disk.destination + '@' + snapshot])
                    if rc != 0:
                        print (stdout)
                        print (stderr)
                    else:
                        deleted_snaps = deleted_snaps +1
                if pzm_common.test:
                    print ("Would have deleted " + str(deleted_snaps) + " snapshots from " + disk.unique_name + " as they are not defined in the restored config")
                else:
                    print ("Deleted " + str(deleted_snaps) + " snapshots from " + disk.unique_name + " as they are not defined in the restored config")


        ##### Important, only delete the snapshot AFTER the config file was re-written, otherwise we would overwrite the deletions with the write command
        snapshots_to_delete_completely = [snap for snap in snapshots_in_config if not snap.keep_snapshot]

        ### Unlock for deleting snapshot
        if len(snapshots_to_delete_completely) > 0:
            execute_command([vm_ct_interaction_command, 'unlock', group.id])

        for snapshot_to_delete in snapshots_to_delete_completely:
            if not pzm_common.test:
                print ("VM/CT ID " + group.id + " - Deleting snapshot " + snapshot_to_delete.name  + " as it doesn't exist on any disk")
            else:
                print ("VM/CT ID " + group.id + " - Woudl delete snapshot " + snapshot_to_delete.name  + " as it doesn't exist on any disk")

            execute_command([vm_ct_interaction_command, 'delsnapshot', group.id, snapshot_to_delete.name])


        print ("VM/CT ID " + group.id + " finished!")
    unlock(args.hostname)
