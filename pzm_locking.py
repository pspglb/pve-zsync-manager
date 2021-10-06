#!/usr/bin/env -S python3 -u

import pzm_common
from pzm_common import log, log_debug, execute_command, execute_readonly_command
import os
import socket
import random
import time

remoteSyncLock = "/var/lib/pve-zsync/manager_sync.lock"
locked = False
remote_locked_here = False
local_locked_here = False


#Check if the local lock (=file "remoteSyncLock") is lockable
#the file on remote and local is the same, in order to be able to do local and remote sync one by one.
def can_get_local_lock():
    if os.path.exists(remoteSyncLock):
        with open(remoteSyncLock, 'r') as lockfile:
            output = lockfile.read()
            log("Local lock is held by " + output + ", have to wait...")
        return False
    else:
        log_debug ("Local lockfile does not exist, can proceed...")
        return True

#Check if the remote lock (=file "remoteSyncLock") is lockable
#the file on remote and local is the same, in order to be able to do local and remote sync one by one.
def can_get_remote_lock(hostname):
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
    presleep = random.uniform(0,60)
    if not pzm_common.test:
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
