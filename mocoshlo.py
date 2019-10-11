#!/usr/bin/env python3

import os
import sys
import yaml
import argparse
import datetime
from pathlib import Path

def get_sunetid(sunetid_arg):
    sunetid = None
    if sunetid_arg:
        sunetid = sunetid_arg
    else:
        with open('config.yaml') as f:
            config = yaml.safe_load(f)
        sunetid = config['sunetid']
    return sunetid

def get_control_path():
    home = str(Path.home()) # Should work on Windows and UNIX.
    if not os.path.exists(f'{home}/.ssh/controlmasters/'):
        os.makedirs(f'{home}/.ssh/controlmasters')
    return f"{home}/.ssh/controlmasters/%C"

def get_server(sunetid):
    return f"{sunetid}@login.sherlock.stanford.edu"

def pull():
    parser = argparse.ArgumentParser(
        description="Download a container from the internet to the Sherlock "
                    "cluster. The container is saved to "
                    "$GROUP_HOME/{sunetid}/opensim-moco/"
                    "opensim-moco_{mocotag}.sif. "
                    "WARNING: This will overwrite existing containers.")
    parser.add_argument('URL', type=str)
    parser.add_argument('--mocotag', type=str, default='',
                        help="The suffix in the name of the saved container. "
                             "If omitted, we use Singularity's default name.")
    parser.add_argument('--sunetid', type=str, default=None,
                        help="SUNetID for logging into Sherlock. Overrides the "
                             "the sunetid field in a config.yaml file in the "
                             "current directory.")

    args = parser.parse_args(sys.argv[2:])
    name = ''
    if ' ' in args.mocotag:
        raise Exception("Cannot have spaces in suffix.")
    if len(args.mocotag):
        name = 'opensim-moco_{args.mocotag}.sif'

    sunetid = get_sunetid(args.sunetid)

    server = get_server(sunetid)
    dir = f'$GROUP_HOME/{sunetid}/opensim-moco'
    os.system(f"ssh {server} 'mkdir -p {dir} && cd {dir} && "
              "export PATH=$PATH:/usr/sbin && "
              f"srun --time=30 singularity pull --force {args.mocotag} {args.URL}'")

def submit():
    parser = argparse.ArgumentParser(
            description="Submit a job to the Sherlock cluster using an "
                        "existing container. "
                        "You must run the sshmaster command first.")
    parser.add_argument('directory', type=str, help="Location of input files.")
    parser.add_argument('--sunetid', type=str, default=None,
                        help="SUNetID for logging into Sherlock. Overrides the "
                             "the sunetid field in a config.yaml file in the "
                             "current directory.")
    parser.add_argument('--duration', type=str, default="00:30:00",
            help="Maximum duration for the job in HH:MM:SS.")
    parser.add_argument('--name', type=str, default="",
            help="A name for the job (default: directory name).")
    parser.add_argument('--note', type=str, default="",
            help="A note to save to the directory (as note.txt).")

    parser.add_argument('--command', type=str, default=None,
                        help=("The job should run the given command. "
                              "Otherwise, the job is to run an OMOCO file named "
                              "setup.omoco."))
    parser.add_argument('--mocotag', type=str, default='latest',
                        help="Use container "
                             "$GROUP_HOME/{sunetid}/opensim-moco/"
                             "opensim-moco_{mocotag}.sif. Default: latest.")
    parser.add_argument('--container', type=str, default=None,
                        help="Absolute path to the singularity container (.sif) "
                             "on the cluster. Overrides --mocotag.")
    parser.add_argument('--exclude', type=str, default=None,
                        action='append',
                        help="Exclude files from copying to the cluster. This is "
                             "passed onto rsync --exclude. "
                             "This argument can be repeated.")
    parser.add_argument('--parallelism', type=int, default=4,
                        help="Number of parallel threads. Default: 4.")


    # TODO: Windows
    # TODO: initial configuring gdrive on Sherlock.
    # TODO: building a docker container from a branch (as a separate step?).
    # TODO: allow customizing where files are saved in Google Drive.
    # TODO: memory seems fixed at 4GB. use mem-per-cpu=4000M?


    args = parser.parse_args(sys.argv[2:])

    directory = args.directory
    if args.name != "":
        name = args.name
    else:
        name = Path(directory).absolute().name
    duration = args.duration
    note = args.note

    if ' ' in name:
        raise Exception("Cannot have spaces in name.")

    sunetid = get_sunetid(args.sunetid)
    control_path = get_control_path()
    server = get_server(sunetid)

    import subprocess
    ssh_is_open = True
    try:
        subprocess.check_call(f'ssh -S {control_path} {server} -O check',
                                  shell=True)
    except subprocess.CalledProcessError:
        ssh_is_open = False
    if not ssh_is_open:
        raise Exception('SSH connection is not open; '
                        'use command sshmaster first.')


    # Check that the directory contains setup.omoco.
    if (not args.command and not os.path.exists(
            os.path.join(directory, 'setup.omoco'))):
        raise Exception(f'setup.omoco is missing from {directory}.')


    if note:
        with open(os.path.join(directory, f'{name}_note.txt'), 'w') as f:
            f.write(note)


    now = datetime.datetime.now()
    date = now.strftime('%Y-%m-%d')
    time = '%s.%i' % (now.strftime('%Y-%m-%dT%H%M%S'), now.microsecond)
    job_directory = '%s-%s' % (time, name)
    print(f"Submitting {job_directory}")
    mocojobs_dir = f"~/nmbl/mocojobs/"
    server_job_dir = f"{mocojobs_dir}{job_directory}"


    container = f'$GROUP_HOME/{sunetid}/opensim-moco/opensim-moco_{args.mocotag}.sif'
    if args.container:
        container = args.container

    command = '/opensim-moco-install/bin/opensim-moco run-tool setup.omoco'
    if args.command:
        command = args.command

    batch = f"""#!/bin/bash
#SBATCH --job-name={name}
#SBATCH --output={name}.out.txt
#SBATCH --error={name}.err.txt
#SBATCH --time={duration}
#SBATCH --mail-type=ALL
#SBATCH --mail-user={sunetid}@stanford.edu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={args.parallelism}
#SBATCH --mem-per-cpu=2000M
#SBATCH --partition=owners,normal
module load gcc/8.1.0

echo "nproc: $(nproc)"
# TODO: set to 2 * nproc?
export OPENSIM_MOCO_PARALLEL=$(nproc)
singularity exec {container} {command}


# Upload results to Google Drive.
module load system gdrive

opensim_moco_folder_id=$(gdrive list --no-header --absolute --name-width 0 --query "name = 'opensim-moco' and trashed = false and 'root' in parents" | cut -d" " -f1)
if [[ -z "$opensim_moco_folder_id" ]]; then
    echo "Creating opensim-moco folder."
    opensim_moco_folder_id=$(gdrive mkdir opensim-moco | cut -d" " -f2)
else
    echo "opensim-moco folder exists."
fi

# Copy results.
gdrive upload --recursive --parent $opensim_moco_folder_id {server_job_dir}

# If the results were successfully uploaded, delete the folder from the
# cluster to save disk space. Can't delete this batch script though.
upload_exit_status=$?
if [ $upload_exit_status -eq 0 ]; then
    find {server_job_dir} -type f -not -name '{name}.*' -delete
fi

mkdir -p {mocojobs_dir}/completed
mv {server_job_dir} {mocojobs_dir}/completed/



"""

    with open(f'{directory}/{name}.batch.sh', 'w') as f:
        f.write(batch)

    # Re-use existing SSH tunnel.
    # Recursively make the job directory.
    os.system(f'ssh -S {control_path} {server} "mkdir -p {mocojobs_dir}"')
    rsync_args = ''
    if args.exclude:
        for exc in args.exclude:
            rsync_args += f'--exclude={exc} '

    print(f"Copying directory '{os.path.abspath(directory)}'...")
    os.system(f"rsync --rsh='ssh -o ControlPath={control_path}' "
              f"--archive --compress --recursive {rsync_args} "
              f"'{directory}/' {server}:{server_job_dir}")

    print("Submitting the job...")
    os.system(f'ssh -S {control_path} {server} '
              f'"cd {server_job_dir} && echo \"{note}\" > note.txt && '
              f'sbatch {name}.batch.sh"')

    print("Job submitted.")


def sshmaster():
    parser = argparse.ArgumentParser(
        description="Start a 2-hour persistent SSH connection to Sherlock.")
    parser.add_argument('--sunetid', type=str, default=None,
                        help="SUNetID for logging into Sherlock. Overrides the "
                             "the sunetid field in a config.yaml file in the "
                             "current directory.")
    args = parser.parse_args(sys.argv[2:])


    sunetid = get_sunetid(args.sunetid)

    control_path = get_control_path()
    server = get_server(sunetid)
    # Create master (-M) SSH session in the background (-f), without running
    # a command (-N), for 2 hours (-o and ControlPersist).
    # https://unix.stackexchange.com/questions/83806/how-to-kill-ssh-session-that-was-started-with-the-f-option-run-in-background
    # -S: Control path.
    ssh_duration_seconds = 2 * 60 * 60
    os.system(f"ssh -o 'ControlPersist {ssh_duration_seconds}' "
              f"-M -f -N -S {control_path} {server}")

def sshexit():
    parser = argparse.ArgumentParser(
        description="Start a persistent SSH connection to Sherlock.")
    parser.add_argument('--sunetid', type=str, default=None,
                        help="SUNetID for logging into Sherlock. Overrides the "
                             "the sunetid field in a config.yaml file in the "
                             "current directory.")
    args = parser.parse_args(sys.argv[2:])
    sunetid = get_sunetid(args.sunetid)
    control_path = get_control_path()
    server = get_server(sunetid)
    os.system(f'ssh -S {control_path} -O exit {server}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Run containerized OpenSim Moco jobs on Sherlock.",
        usage="mocoshlo <command> [<args>]")
    help = ("Command to run. "
            "'pull': Download a container from the internet to the Sherlock "
            "cluster (experimental). "
            "'submit': Submit a job to the Sherlock cluster using an existing "
            "container. "
            "'sshmaster': Start a persistent SSH session; required before submit. "
            "'sshexit': End the persistent SSH session. "
    )
    parser.add_argument('command', type=str, help=help,
                        choices=('pull', 'submit', 'sshmaster', 'sshexit'))
    args = parser.parse_args(sys.argv[1:2])
    if args.command == 'pull':
        pull()
    elif args.command == 'submit':
        submit()
    elif args.command == 'sshmaster':
        sshmaster()
    elif args.command == 'sshexit':
        sshexit()
    else:
        raise RuntimeError()

