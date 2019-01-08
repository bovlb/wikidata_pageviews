"""Utilities for dealing with the jobs on the grid"""

from typing import Iterable, NamedTuple
import subprocess
import xml.etree.ElementTree as ET
import time
import sys
import argparse
import requests

class Job(NamedTuple):
    num: int
    name: str
    state: str
    long_state: str


def job_status() -> Iterable[Job]:
    """Check jobs on queue and yields job objects

    Yields:
        job: Job object
    """
    cmd = [ '/usr/bin/qstat', '-xml' ]
    try:
        xml = subprocess.check_output(cmd, shell=False)
    except subprocess.CalledProcessError as e:
        print("Output:", e.output)
        print("Error:", e.stderr)
        raise
    tree = ET.fromstring(xml)
    for job in tree.iter('job_list'):
        long_state = job.attrib['state']
        num = job.find('JB_job_number').text
        name = job.find('JB_name').text
        state = job.find('state').text
        j = Job(num, name, state, long_state)
        yield(j)


def monitor_jobs(sleep_secs=60):
    """Continuously monitor jobs, yielding changes""" 
    previous = {}
    while True:
        jobs = {j.num:j for j in job_status()}
        for num, job in jobs.items():
            if num not in previous:
                yield f"New job {num} ({job.name}): state={job.long_state}"
            elif previous[num].long_state != job.long_state:
                yield f"Changed state job {num} ({job.name}): {previous[num].long_state} -> {job.long_state}"
        for num, job in previous.items():
             if num not in jobs:
                 yield f"Completed job {num} ({job.name})"
        previous = jobs
        time.sleep(sleep_secs)


def print_jobs():
    for job in job_status():
        print(job)

def parse_args(argv=None):
    """Wrapper for argparse.ArgumentParser()"""
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser()
    #parser.add_argument('-v', '--verbose', action='store_true', help='Increases log level to INFO')
    #parser.add_argument('-d', '--debug', action='store_true', help='Increases log
    parser.add_argument("--slack-hook", action='store', 
                        help="URL like https://hooks.slack.com/services/blah/blah/blah")
    parser.add_argument('--slack-channel', action='store',
                        help="channel like @general or user like #bob")
    args = parser.parse_args(argv)
    return args


def slack_message(message, hook, channel):
    data = dict(text=message)
    if channel:
        data['channel'] = channel
    r = requests.post(hook, json=data)
    r.raise_for_status()


def monitor():
    args = parse_args()
    for message in monitor_jobs():
        print(message)
        if args.slack_hook:
            slack_message(message, args.slack_hook, args.slack_channel)

if __name__ == "__main__":
    monitor()
