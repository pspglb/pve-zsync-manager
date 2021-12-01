#!/usr/bin/env -S python3 -u

import logging
import sys
import argparse
import traceback

from pzm_status import read_from_json
from pzm_restore import gather_restore_data, restore
from pzm_sync import sync
from pzm_sanitize import sanitize
from pzm_locking import unlock
from pzm_common import log, log_debug, log_verbose
import pzm_common

def main():
    if len(sys.argv) <= 2 and "status" not in sys.argv:
        #status is the only method which can stand alone without params, everything also should append --help
        sys.argv.append("--help")

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
    syncArgsParser.add_argument("--debug", help="Enable debug mode", action="store_true")
    syncArgsParser.add_argument("--test", help="Only test the functionality, do not actually execute anything", action="store_true")

    # Command: status - Arguments
    statusArgsParser = argparse.ArgumentParser()

    statusArgsRequired = statusArgsParser.add_argument_group('required Arguments')
    statusArgsRequired.add_argument("status")
    statusArgsParser.add_argument("--verbose", help="Enable verbose mode", action="store_true")
    statusArgsParser.add_argument("--debug", help="Enable debug mode", action="store_true")
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
    restoreArgsParser.add_argument("--debug", help="Enable debug mode", action="store_true")
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
    sanitizeArgsParser.add_argument("--debug", help="Enable debug mode", action="store_true")
    sanitizeArgsParser.add_argument("--test", help="Only test the functionality, do not actually execute anything", action="store_true")


    if "sync" in sys.argv:
        args = syncArgsParser.parse_args()
        pzm_common.initialize("sync_" + args.backupname)
        
        pzm_common.verbose = args.verbose
        pzm_common.debug = args.debug
        pzm_common.test = args.test
        
        if pzm_common.verbose and not pzm_common.debug:
            log ("Verbose mode")
        if pzm_common.debug:
            log ("Debug mode")
        if pzm_common.test:
            log ("Test mode")
        try:
            log_verbose ("Sync started with: " + ' '.join(sys.argv[0:]))
            sync(args)
        except KeyboardInterrupt:
            log ("Interupted by User", logging.WARN)
            unlock(args.hostname)
        except Exception: #Also unlock at any other exception
            log (traceback.format_exc(), logging.CRITICAL)
            unlock(args.hostname)


    elif "status" in sys.argv:
        args = statusArgsParser.parse_args()
        pzm_common.initialize("status", False)
        read_from_json(args.plain)


    elif "restore" in sys.argv:
        args = restoreArgsParser.parse_args()
        pzm_common.initialize("restore")
        
        pzm_common.verbose = args.verbose
        pzm_common.debug = args.debug
        pzm_common.test = args.test
        
        if pzm_common.verbose and not pzm_common.debug:
            log ("Verbose mode")
        if pzm_common.debug:
            log ("Debug mode")
        if pzm_common.test:
            log ("Test mode")
        try:
            disk_groups = gather_restore_data(args)
        except KeyboardInterrupt:
            log ("Interupted by User", logging.WARN)
            sys.exit(1)
        if disk_groups is not None:
            try:
                restore(args, disk_groups)
            except KeyboardInterrupt:
                log ("Interupted by User", logging.WARN)
                unlock(args.hostname)
            except Exception: #Also unlock at any other exception
                log (traceback.format_exc(), logging.CRITICAL)
                unlock(args.hostname)


    elif "sanitize" in sys.argv:
        args = sanitizeArgsParser.parse_args()
        pzm_common.initialize("sanitize")
        
        pzm_common.verbose = args.verbose
        pzm_common.debug = args.debug
        pzm_common.test = args.test
        
        if pzm_common.verbose and not pzm_common.debug:
            log ("Verbose mode")
        if pzm_common.debug:
            log ("Debug mode")
        if pzm_common.test:
            log ("Test mode")
        try:
            sanitize(args)
        except KeyboardInterrupt:
            log ("Interupted by User", logging.WARN)
        except Exception: 
            log (traceback.format_exc(), logging.CRITICAL)

    #If no command is given
    else:
        print ("ERROR: no or invalid command sepcified!")
        print ("")
        print ("USAGE: ")
        print ("    " + sys.argv[0] + " status [OPTIONS]")
        print ("    " + sys.argv[0] + " sync [OPTIONS]")
        print ("    " + sys.argv[0] + " restore [OPTIONS]")
        print ("    " + sys.argv[0] + " sanitize [OPTIONS]")



if __name__ == "__main__":
    main()





