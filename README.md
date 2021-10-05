**Introduction:**

Proxmox VE implements pve-zsync for backing up ZFS Datasets and Volumes to a remote or local ZFS Dataset.
However there are some limitations:
- Can only Backup one VM/CT at a Time (with one ID)
- Does not support Replication or RAW (encrypted) Syncs with dataset properties
- Needs manual interaction to cron file - e.g. if a new VM or CT is created, a new cronline has to be added with the corresponding VM/CT ID
- A restore has to be done manually, for every single dataset
- Failures due to for example a network issues are not retried.

pve-zsync-manager solves that limitations, by providing the possibilty to backup all IDs with properties and all snapshots (if wanted) with a single line.
Also it provides options to restore VM/CTs from backup location and sanitize Backuplocations if something went wrong during a backup.
It will also retry a failed backup if specified. Before each retry it will sanitize the remote side, so there are no zfs related issues, like remote side has newer snapshots.
pve-zsync-manager provides a simple locking mechanism which should only allow one disk operation (read or write) from a location and also to a location.
If this locking mechanism fails, the only issue would be a perfomance drop as the disk will to read/write at the same time or read two different datasets at the same time.

Due to the locking mechanism and random wait times before starting, it is safe execute all backup commands (e.g. two different backups to different locations) and on serveral hosts at the same time.
One process will get the lock for the local and remote host (All or nothing), the others will wait till they can get all locks (also All or Nothing).
Backups to Localhost are also possible, as the locking mechanism checks which hostname holds the lock, and proceeds if it's itself.

Restore will parse all existing volumes and datasets on a given remote datset and asks for an action on every single one separately!
It will not override anything unless you answer the final "Is everything correct" question with yes.
You can filter remote datasets with "--filter"

If datasets are encrypted and should be restored, it is adviced to write you zfs-passphrase to a file (echo -n "<passphrase"> > /zfs-password) and provide the location with --keyfile.
This results in automatically loading the dataset key and inheriting the parent dataset key if possible

Almost every option supports a "--test" agrument. It will perform any neccessary read operation, but will not actually write anything.
One can view all executed commands (or commands that would be executed without --test) with --verbose

**Notes about the pve-zsync patch**

The patch includes mainly four things:
- It firstly locks the VM/CT before each sync, und unlocks it afterwards
- Changes the location of the config file which is also backuped
- Adds the ability to do replication snapshots, which include all intermediate snapshots
- Adds the ability to do raw sync, for encrypted datasets

**Installation:**

Install Python3

Install pve-zsync and patch /usr/sbin/pve-zsync with Patchfile
( patch /usr/sbin/pve-zsync < pve-zsync-raw-replicate-locks.patch )

Clone to anywhere on a Proxmox VE Host, and make a symlink to pve-zsync-manager.py in a place where PATH can find it.
e.g.: ln -s /opt/pve-zsync-manager/pve-zsync-manager.py /usr/sbin/pve-zsync-manager

Before running, make sure the SSH key is copied to the remote server, for passwordless login. (This is required)
ssh-copy-id root@backupserver01.local

**Usage:**

USAGE:

    /usr/sbin/pve-zsync-manager status [OPTIONS]
    /usr/sbin/pve-zsync-manager sync [OPTIONS]
    /usr/sbin/pve-zsync-manager restore [OPTIONS]
    /usr/sbin/pve-zsync-manager sanitize [OPTIONS]

-----------------------------------------------------------------
    pve-zsync-manager status --help
    usage: pve-zsync-manager [-h] [--verbose] [--plain] status

    optional arguments:
      -h, --help  show this help message and exit
      --verbose   Enable verbose mode
      --plain     Print text without colors

    required Arguments:
      status
--------------------------------------------------------------------------------
    pve-zsync-manager sync --help
    usage: pve-zsync-manager [-h] --hostname HOSTNAME --zfspool ZFSPOOL
                         --backupname BACKUPNAME --ids IDS
                         [--dest-config-path DEST_CONFIG_PATH] [--replicate]
                         [--raw] [--maxsnap MAXSNAP] [--properties]
                         [--verbose] [--test]
                         sync

    optional arguments:
      -h, --help            show this help message and exit
      --dest-config-path DEST_CONFIG_PATH 
                        Path to store VM/CT config files on destination host
      --replicate           Set if Dataset should be replicated with all Snapshots and Properties
      --raw                 Send Dataset in Raw (Encrypted) mode
      --maxsnap MAXSNAP     Keep given amount of snapshots
      --properties          Send Dataset with properties (If Dataset is encrypted, raw has to be set too!)
      --retries RETRIES     Retry amount of failed backups
      --prepend-storage-id  Prepends any VM/CT Disk with it's corresponding pve-storage id 
                            (Adds an additinal zfs dataset layer)
      --verbose             Enable verbose mode
      --test                Only test the functionality, do not actually execute anything

    required Arguments:
      sync
      --hostname HOSTNAME   Destination Host for Backups
      --zfspool ZFSPOOL     ZFS Destination Pool for Backups
      --backupname BACKUPNAME
                          Name of PVE-ZSYNC Snapshots
      --ids IDS             Use VM/CT Numbers, separated with commas, or use
                         "all". Exclude with -number e.g --ids all,-1000

---------------------------------------------------------------------------------
    pve-zsync-manager restore --help
    usage: pve-zsync-manager [-h] --hostname HOSTNAME --zfs-source-pool
                         ZFS_SOURCE_POOL --backupname BACKUPNAME --config-path
                         CONFIG_PATH [--keyfile KEYFILE] [--test] [--verbose]
                         [--filter FILTER]
                         restore

    optional arguments:
      -h, --help            show this help message and exit
      --keyfile KEYFILE     Path to keyfile, needed for inheriting the ZFS-Key
      --test                Only test the functionality, do not actually execute anything
      --verbose             Enable verbose mode
      --filter FILTER       Filter for given string

    required Arguments:
      restore
      --hostname HOSTNAME   Backup-Source Hostname
      --zfs-source-pool ZFS_SOURCE_POOL
                        ZFS Source Pool (Same as destination Pool with "sync")
      --backupname BACKUPNAME
                        Name of PVE-ZSYNC Snapshots (Same as with "sync")
      --config-path CONFIG_PATH
                        Path to restore VM/CT config files from
---------------------------------------------------------------------------------
    pve-zsync-manager sanitize --help
    usage: pve-zsync-manager [-h] --hostname HOSTNAME --zfspool ZFSPOOL
                         --backupname BACKUPNAME --ids IDS [--verbose]
                         [--test]
                         sanitize

    optional arguments:
      -h, --help            show this help message and exit
      --verbose             Enable verbose mode
      --test                Only test the functionality, do not actually execute anything

    required Arguments:
      sanitize
      --hostname HOSTNAME   Host to sanitize
      --zfspool ZFSPOOL     ZFS Pool to sanitize
      --backupname BACKUPNAME
                        Name of PVE-ZSYNC Snapshots
      --ids IDS             Use VM/CT Numbers, separated with commas, or use
                        "all". Exclude with -number e.g --ids all,-1000


**Examples:**

pve-zsync-manager sync --ids all,-101,-100,-20115 --hostname backupserver01.local --backupname backupserver01-backup-raw --zfspool backuppool/proxmox01/VM-CT-Backup --replicate --dest-config-path /backuppool/proxmox01 --raw --properties --maxsnap 96

pve-zsync-manager sync --ids all,-101,-100,-20115 --hostname offsitebackupserver01.local --backupname offsite-backup-raw --zfspool offsite-backuppool/proxmox01/VM-CT-Backup --replicate --dest-config-path /offsite-backuppool/proxmox01 --raw --properties --maxsnap 96

pve-zsync-manager sync --ids 20103 --hostname backupserver01.local --backupname backupserver01-backup-raw --zfspool backuppool/proxmox01/VM-CT-Backup --replicate --dest-config-path /backuppool/proxmox01 --raw --properties --maxsnap 96

pve-zsync-manager sync --ids proxmox01.local:1001 --hostname localhost --backupname template-sync-backupserver01 --zfspool rpool/vmdata



pve-zsync manager status

pve-zsync manager status --plain



pve-zsync-manager restore --hostname backupserver01 --zfs-source-pool backuppool/proxmox01/VM-CT-Backup --backupname backupserver01-backup-raw --config-path /backuppool/proxmox01/VM-CT-Backup --verbose --filter 20002-disk-1 --keyfile /zfs-password

pve-zsync-manager sanitize --hostname offsitebackupserver01 --zfspool offsite-backuppool/proxmox01/VM-CT-Backup --backupname offsite-backup-raw --ids all,-101,-100,-20115 --verbose

pve-zsync-manager sanitize --hostname offsitebackupserver01 --zfspool offsite-backuppool/proxmox01/VM-CT-Backup --backupname offsite-backup-raw --ids 20002 --verbose
