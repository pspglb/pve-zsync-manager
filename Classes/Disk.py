#!/usr/bin/env python3

import pzm_common
from pzm_common import execute_readonly_command, log
import sys

#Disc class for the restore function.
#Each disk has a Name/ID, latest snaoshot, destination (aka pool) and a vm/ct config file
class Disk:
    @staticmethod
    def get_unique_name_from_config_line(config_line):
        #from for example rootfs: vmssd:subvol-100-disk-0,mountpoint=.... to vmssd:subvol-100-disk-0
        return config_line.split(',')[0].split(':',1)[1].replace(' ','')
    
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
            disk = Disk.get_unique_name_from_config_line(diskconfig[0])
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
