#!/usr/bin/env python3
"""
Snakemake SLURM submit script.
"""
import warnings  # use warnings.warn() rather than print() to output info in this script
import argparse
import ssh_utils
import sys

def readcmds(file):
    ret=[]
    if file=='-':
        fi = sys.stdin
    else:
        fi = open(file, "r")
    for line in fi.readlines():
        line = line.strip()
        if not line: continue
        ret.append(line)
    return ret


def submit(args, cmds):
    for cmd in cmds:
        # submit_job_cmd(cmd, threads, pwd = os.getcwd(), ip=ZZSERVER_IP, port=ZZSERVER_PORT)
        ssh_utils.submit_job_cmd(cmd, args.cpu,
                                 ip=args.server_host, port=args.server_port)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Submit command lines to slurm.")
    parser.add_argument("-j", "-c","--cpu_num",type = int,default = 1,dest = "cpu",help="set the number of cpu per task,default = 1")
    parser.add_argument("-i","--infile", required= True, dest = "infile",help="offer the file of commands ('-' for stdin for pipeline)")
    parser.add_argument("-h","--host", dest = "server_host", help="ssh-server host", default='127.0.0.1')
    parser.add_argument("-p","--port", type = int, dest = "server_port",help="ssh-server port", default='23456')
    args = parser.parse_args()
    cmds = readcmds(args.infile)
    submit(args, cmds)
