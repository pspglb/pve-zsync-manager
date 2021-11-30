
#!/usr/bin/env python3

import sys
import pzm_common
from Classes.Backed_Up_Disk import Backed_Up_Disk
from Classes.Non_Backed_Up_Disk import Non_Backed_Up_Disk
from Classes.Disk import Disk
from pzm_common import execute_readonly_command, log, log_debug



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
        parsed_non_backed_up_disk_config_lines = [element for element in all_disks if len([backed_up_disk for backed_up_disk in self.backed_up_disks if Disk.get_unique_name_from_config_line(element) in backed_up_disk.unique_name]) == 0]


        for config_line in parsed_non_backed_up_disk_config_lines:
            self.non_backed_up_disks.append(Non_Backed_Up_Disk(configs_path + '/' + self.get_last_config(), config_line, self.type))

    def __eq__(self,other):
        if not isinstance(other, Disk_Group):
            # don't attempt to compare against unrelated types
            return NotImplemented
        return self.id == other.id
