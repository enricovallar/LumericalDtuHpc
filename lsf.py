"""
Copyright (c) 2025 Ansys Inc.
This program is commercial software: you can use it under the terms of the
Ansys License Agreement as published by Ansys Inc. You should have
received a copy of the Ansys License Agreement along with this program.

Except as expressly permitted in the Ansys License Agreement, you may not
modify or redistribute this program.
"""


import subprocess
import sys
from time import sleep
from os.path import split, splitext, expanduser, join
import re
from getpass import getuser
from job_scheduler_utils import input_data, log, posix_path, call, check_output

# Populate the user input file job_scheduler_input.json located in the config directory 
# (~/.config/Lumerical on linux, %APPDATA%\Lumerical on Windows)
#
# USER_NAME will be dynamically assigned with: import getpass; getpass.getuser() if left empty
#
# Set use_ssh and use_scp JSON fields to "1" or "0"
#
# CLUSTER_CWD ex: "$HOME/Project_1/" NFS directory shared by all nodes, can be '$HOME/' or a full path, ending in '/'
#
# PATH_TRANSLATION is empty by default, paths are 1:1 match. Use this in the case that the GUI is running on the cluster already.
# 
# PATH_TRANSLATION ex: "X:/Working Dir/", "/share/working/dir/" 
# Use unix-style path deliminters '/'. Use this in the case that there is a shared file system on a windows machine to a linux cluster,
# or if the host machine has different mount points for the shared directory.


POOLTIME = 30 # seconds to wait between job status checks
USE_SSH = bool(input_data()['use_ssh'])
USE_SCP = bool(input_data()['use_scp'])
CLUSTER_CWD = input_data()['cluster_cwd']
if USE_SSH:
    USER_NAME = getuser() if not input_data()['user_name'] else input_data()['user_name'] 
    SSH_LOGIN = f"{USER_NAME}@{input_data()['master_node_ip']}"
    SSH_KEY = expanduser(input_data()['ssh_key'])    
if not USE_SCP:
    PATH_TRANSLATION = tuple(input_data()['path_translation'])


def ssh_wrapper(arr):
    cmd = ''
    if(CLUSTER_CWD):
        cmd += f'cd {CLUSTER_CWD};' # if the directory does not exist this will fail silently. mk_remote_dir must be called explicitly.
    cmd += "bash -l -c "
    cmd += ' '.join(arr)
    cmd = ['ssh', '-i', SSH_KEY, SSH_LOGIN, cmd]
    return cmd


def mk_remote_dir(d=CLUSTER_CWD):
    assert(d)
    cmd = ssh_wrapper(['mkdir', '-p', d])

    log(f'Creating remote dir: {d}. Calling command: {" ".join(cmd)}') # 
    call(cmd)


def put(fsp_file):
    cmd = ['scp', '-i', SSH_KEY, fsp_file, SSH_LOGIN + ':' + CLUSTER_CWD]
    log(f'Copying local file "{fsp_file}" to remote host. Calling command: {" ".join(cmd)}')
    call(cmd)


def get(fsp_basename):
    cmd = ['scp', '-i', SSH_KEY, SSH_LOGIN + ':' + CLUSTER_CWD + fsp_basename + '*', '.']
    log(f'Copying remote file to local host. Calling command {" ".join(cmd)}')
    call(cmd)


def remote_path_substitution(local_path):
    filepath, filename = split(local_path)
    remote_path = None
    if USE_SCP:
        remote_path = CLUSTER_CWD + filename # SCP defults to $HOME
    else:
        remote_path = join(filepath.replace(PATH_TRANSLATION[0], PATH_TRANSLATION[1]), filename) # abspath forces unix-style delimiters
    remote_path = posix_path(remote_path)
    log(f'Translated local path "{local_path}" to remote path "{remote_path}"')
    return remote_path


def parse_submission_script(submission_script_lines):
# expects a quoted path for a file with extension ['.fsp', '.icp', '.lms', '.ldev']
    local_path = ''
    filename = ''
    basename = ''
    for i in range(len(submission_script_lines)):
        line = submission_script_lines[i]
        # filepath must be a double-quoted string
        quoted_args = re.findall('"([^"]*)"', line)
        if (len(quoted_args) > 0):
            for arg in quoted_args:
                if any(arg.endswith(file_extension) for file_extension in ['.fsp', '.icp', '.lms', '.ldev']):
                    local_path = arg
                    filename = split(local_path)[1]
                    basename = splitext(filename)[0]
                    submission_script_lines[i] = line.replace(local_path, remote_path_substitution(local_path))
                    break

    if not basename:
            raise Exception("A project file (.fsp, .ipc, .lms, .ldev) was not found in the provided arguments: {}".format(submission_script))
    assert(local_path != '')
    assert(filename != '')
    assert(basename != '')
    return submission_script_lines, local_path, filename, basename


def run_job(submission_script_lines, submission_command):
    assert((len(submission_command) > 0))

    submission_script_lines, local_path, filename, basename = parse_submission_script(submission_script_lines)

    if USE_SSH and USE_SCP:
        put(local_path)

    job_id = submit_job(submission_script_lines, submission_command, basename)
    sleep(0.5)
    assert(job_in_queue(job_id))
    print("<status>QUEUED</status>")
    sys.stdout.flush()

    while True:
        if job_in_queue(job_id):
            log(f"Job {job_id} is no longer in the queue")
            break
        log(f"Job {job_id} is not in the queue, checking status in {POOLTIME} seconds")
        sleep(POOLTIME)

    while True: 
        status = job_status(job_id)
        log(f"Job {job_id} status: {status}")
        if status == 'DONE' or status == None :
            status = 'DONE'
            print("<status>DONE</status>")
            # print("<simComplete/>")
            # print("<complete/>")
            sys.stdout.flush()
            break
        log(f"Job {job_id} is still running, checking status in {POOLTIME} seconds")
        sleep(POOLTIME)
    
    

    if USE_SSH and USE_SCP:
        get(basename)


def kill_job(job_id):
    cmd = ['bkill', job_id]
    if USE_SSH:
        cmd = ssh_wrapper(cmd)
    log("Kill job: " + ' '.join(cmd))
    call(cmd)


def submit_job(submission_script, submission_command, job_name):
    cmd = submission_command + ['-J', job_name]
    if USE_SSH:
        cmd = ssh_wrapper(cmd)

    log('Submission Command: ' + ' '.join(cmd))
    log('Submission Script:\n' + '\n'.join(submission_script))
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE,stdout=subprocess.PIPE, encoding='utf8', universal_newlines=True)
    p.stdin.buffer.write('\n'.join(submission_script).encode()) # ensure unix style line endings are used
    p.stdin.close()

    result = p.stdout.readlines()
    log('Submission Response:\n' + '\n'.join(result))

    result = result[0].split()
    job_id = result[1].replace('<', '').replace('>', '')
    assert(job_id.isdigit())
    log(f'Submission successful, Job ID: {job_id}')
    return job_id


def job_in_queue(job_id):
    cmd = ['bjobs', job_id]
    if USE_SSH:
        cmd = ssh_wrapper(cmd)
    job_status = check_output(cmd)
    for line in job_status.splitlines():
        possible_job_id = line.strip().split()[0]
        if possible_job_id == str(job_id):
            return True
    return False


def job_status(job_id):
    cmd = ['bjobs', job_id]
    if USE_SSH:
        cmd = ssh_wrapper(cmd)
    log(f'Checking job status for job ID: {job_id}. Calling command: {" ".join(cmd)}')
    job_status = check_output(cmd)
    for line in job_status.splitlines():
        possible_job_id = line.strip().split()[0]
        if possible_job_id == str(job_id):
            status = line.strip().split()[5]
            status.strip()
            log(f"Job {job_id} status line: {line.strip()}")
            log(f" Job {job_id} status: {status}")
            if status == 'PEND':
                print("<status>PENDING</status>")
            if status  == 'RUN':
                print("<status>RUNNING</status>")
            if status  == 'DONE':
                print("<simComplete/>")
                print("<complete/>")
            sys.stdout.flush()
            return status
        



if __name__ == '__main__':

    submission_command = sys.argv[1:]
    submission_script = sys.stdin.read().splitlines()
    if(CLUSTER_CWD):
        mk_remote_dir()
    run_job(submission_script, submission_command)

