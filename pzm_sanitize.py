import time
import datetime
import os

from pzm_common import execute_readonly_command, execute_command, log, log_debug

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
