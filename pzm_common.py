#!/usr/bin/env -S python3 -u

import subprocess
import datetime
import sys
import logging
from logging.handlers import RotatingFileHandler


#Colors for fancy output
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


def initialize(name, enable_logging=True):
    global debug
    global verbose
    global test
    global statusJsonFile
    global considered_empty
    global logger
    global log_to_file
    debug = False
    verbose = False
    test = False
    statusJsonFile = "/var/lib/pve-zsync/manager_sync_state"
    considered_empty = ['\n', '', " "]

    #Logging
    logging.addLevelName(logging.DEBUG + 5, "VERBOSE")
    logging.VERBOSE = logging.DEBUG + 5
    logging.addLevelName(logging.INFO + 1, "USRINPUT")
    logging.USRINPUT = logging.INFO + 1
    log_file_handler = RotatingFileHandler(filename=f"/var/log/pzm_{name}.log", encoding="utf-8", maxBytes=10485760, backupCount=10)
    log_file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    log_file_handler.setFormatter(log_file_formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    if enable_logging:
        logger.addHandler(log_file_handler)
        logger.log(logging.INFO, "==================== Started with " + name + " command ====================")

#Log to stdout
def log(data, severity = logging.INFO):
    global logger
    global log_to_file
    if severity == logging.DEBUG:
        print ("DEBUG - " + str(data))
    else:
        print(str(data))
    logger.log(severity, data)

#Log to stdout if global debug or verbose variable is set
def log_verbose(data):
    global verbose
    global debug
    global logger
    if verbose or debug:
        log(str(data), logging.VERBOSE)
    else:
        logger.log(logging.VERBOSE, data)

#Log to stdout if global debug variable is set
def log_debug(data):
    global debug
    global logger
    if debug:
        log(str(data), logging.DEBUG)
    else:
        logger.log(logging.DEBUG, data)

#Userinput should be displayed and logged
def log_input(data):
    global logger
    input_data = input(data)
    logger.log(logging.USRINPUT, str(data) + str(input_data))
    return input_data

#Execute command will not alter anything. These commands can be executed as normal in "TEST" mode
def execute_readonly_command(command):
    log_debug ("Executing command: " + " ".join(command))
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")
    return 0, "", ""

#Execute command which will definetly alter something. Will not be executed in "TEST" mode
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

#Get VM or CT ids from command. command will be either lxc or pct, including ids are numbers, excluding ids are numbers which were given with a heading minus
def get_ids(command, including, excluding):
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
        raise Exception ("(SSH) Error while getting zfs list names " + stderr)
    else:
        if zfspool not in stdout:
            raise Exception ("ZFS Pool " + zfspool + " does not exist on " + hostname)
        else:
            return stdout

