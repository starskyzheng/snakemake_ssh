#!/usr/bin/env python3
#        path = os.path.join(dirname(__file__), os.path.expandvars(path))
# by: zzy
# 2021/11/11

import socket
import json
import threading
import subprocess as sp
import time
import re
import os
import sys
import signal
import yaml
import pickle
import argparse

#from Common.dir_config import *

####################################################################################
####################################################################################
####################################################################################
####################################################################################

lock_jobs = threading.Lock()

class Config:
    def __init__(self, yaml_file):
        self.pickleFile = "{}.jobs.pickle".format(__file__)
        self.yaml_file = yaml_file
        self.yaml_file_read_time = -9999
        self.nodes={}
        self.jobs={}
        self.nodes_used={}
        self.lock_nodes = threading.Lock()
        self.JOBS_INIT = 1 # default init
        self.ijob_max = self.JOBS_INIT - 1
        self.ijob_next =  self.ijob_max + 1
        self.read_config_yaml()
        self.jobs_pickle_load()

    def read_config_yaml(self):
        print("Loading config yaml from: {}".format(self.yaml_file))
        self.yaml_file_read_time = os.path.getmtime(self.yaml_file)
        file = open(self.yaml_file, 'r', encoding="utf-8")
        file_data = file.read()
        file.close()
        data = yaml.load(file_data, Loader=yaml.FullLoader)
        if not data['nodes']:
            print("{} contains no node info!".format(self.yaml_file))
            exit(-1)
        self.port = data['port']
        self.nodes = data['nodes']
        if data.__contains__('JOBpickle'):
            self.pickleFile = data['JOBpickle']
        if data.__contains__('JOBS_INIT'):
            self.JOBS_INIT = data['JOBS_INIT']
        if data.__contains__('cmd_prefix'):
            self.cmd_prefix = data['cmd_prefix']
        if data.__contains__('debug'):
            self.debug = data['debug']
        self.node_max_threads = max(self.nodes.values())
        return 0

    def jobs_pickle_load(self):
        if os.path.exists(self.pickleFile):
            print("load JOBs FROM {}".format(self.pickleFile))
            f=open(self.pickleFile, 'rb')
            self.jobs=pickle.load(f)
            self.JOBS_INIT = max(self.jobs.keys()) + 1
            self.ijob_max = self.JOBS_INIT
            self.ijob_next = self.ijob_max + 1
            self.cmd_prefix = ''
            self.debug = 0
            f.close()

    def jobs_pickle_save(self):
        f=open(self.pickleFile, 'wb')
        pickle.dump(self.jobs, f)
        f.close()

    def update_read_yaml_ifneed(self):
        now_mod_time = os.path.getmtime(self.yaml_file)
        if now_mod_time != self.yaml_file_read_time:
            if self.lock_nodes.acquire():
                self.read_config_yaml()
            self.lock_nodes.release()

    def find_usable_node(self, need):
        self.update_read_yaml_ifneed()
        node_ret = None
        if self.lock_nodes.acquire():
            for node, all in self.nodes.items():
                used = self.nodes_used.get(node, 0)
                if (all >= need + used):
                    self.nodes_used[node] = used + need
                    node_ret = node
                    break
        self.lock_nodes.release()
        return node_ret

    def relase_res(self, node, threads):
        if self.lock_nodes.acquire():
            self.nodes_used[node] = self.nodes_used[node] - threads
        self.lock_nodes.release()

class Watcher(): # Ctrl-C to exit
    def __init__(self):
        self.child = os.fork()
        if self.child == 0:
            return
        else:
            self.watch()

    def watch(self):
        try:
            os.wait()
        except KeyboardInterrupt:
            self.kill()
        sys.exit()

    def kill(self):
        try:
            if lock_jobs.acquire():
                os.kill(self.child, signal.SIGKILL)
        except OSError:
            pass


def ssh_run(job):
    node = job['node']
    threads = job['threads']
    jobid = job['jobid']
    cmd = job['cmd']
    isdone=0
    try:
        #print( "job {} start on {} [{}]".format(jobid, node, job['run_start_time']) )
        print( "job {} start on {}".format(jobid, node) )
        cmd = ["ssh"] + [node] + [cmd]
        res = sp.run(cmd, stdout=sp.PIPE,stderr=sp.PIPE)
        #print("!!!! {}".format(res.decode()))
        if res.returncode ==0:
            isdone=1
    except sp.CalledProcessError as e:
        #print("???? {}".format(e))
        isdone=0
    config.relase_res(node, threads)
    if lock_jobs.acquire():
        job['run_end_time'] = time.time()
        job['run_stderr'] = res.stderr
        job['run_stdout'] = res.stdout
        if isdone==1: # OK
            job['status'] = 'done'
        else: # not ok
            job['status'] = 'fail'
        config.jobs_pickle_save()
    lock_jobs.release()
    print( "job {} done on {}:  {}".format(jobid, node, job['status']) )
    return(isdone)


def recv_job(jdatparm):
    try:
        infos = jdatparm[0]
        cmd = jdatparm[1]
        if not infos['threads']:
            infos['threads']=1
    except:
        return(-1, 'FAIL'.encode("utf-8"))
    threads = infos.get('threads', 1)
    pwd = infos.get('pwd', '/')
    infos['status'] = 'pending'
    cmd = "{} cd {}; {}".format(config.cmd_prefix, pwd, cmd)
    if lock_jobs.acquire():
        config.ijob_max = config.ijob_max + 1
        next_ijob = config.ijob_max # next jobid
        config.jobs[next_ijob] = infos
        config.jobs[next_ijob]['cmd'] = cmd
        config.jobs[next_ijob]['recv_time'] = time.time()
        config.jobs_pickle_save()
    lock_jobs.release()
    print("Got new job {} : `{}` || threads`{}` || pwd`{}` || cmd`{}`".format(next_ijob, config.cmd_prefix, threads, pwd, cmd ))
    sendback = "OK:{}".format(next_ijob).encode("utf-8")
    return(0, sendback)


def query_job(parm):
    try:
        jobid = int(parm)
    except Exception as e:
        print("Error query: not a num!")
        return(-1, 'Bad_JobID'.encode("utf-8"))
    try:
        stat = config.jobs[jobid]['status']
        return(0, stat.encode("utf-8"))
    except:
        return(-1, 'Bad_JobID'.encode("utf-8"))

def node_info():
    config.update_read_yaml_ifneed()
    ret = [ '{}\t{}\t{}\t{}'.format(node, config.nodes[node],
                                config.nodes_used.get(node, 0),
                                config.nodes[node] - config.nodes_used.get(node, 0) )
                    for node in config.nodes ]
    ret.insert(0, 'node\tAll\tused\tAva')
    return(0, '\n'.join(ret).encode("utf-8"))


def init_socket():
    print("----- Open socket: port {} ----".format(config.port))
    tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_server_socket.bind(("", config.port))
    tcp_server_socket.listen(1)
    while(True):
        status = -1
        new_client_socket, client_addr = tcp_server_socket.accept()
        if config.debug: print("-----Got a client----")
        recv_data = new_client_socket.recv(10240)
        recv_data = recv_data.decode("utf-8").strip()
        if config.debug: print(recv_data)
        if recv_data == 'info':
            status, sendback = node_info()
            new_client_socket.send(sendback)
            new_client_socket.close()
            continue
        try:
            jdat = json.loads(recv_data)
            jdatcmd = jdat[0]
            jdatparm = jdat[1]
        except:
            print("bad input! not json!: {}".format(recv_data))
            new_client_socket.send("BAD_INPUT".encode("utf-8"))
            status = -1
            new_client_socket.close()
            continue
        sendback = 'BadCMD'.encode("utf-8")
        if jdatcmd == 'submit':
            status, sendback = recv_job(jdatparm)
        elif jdatcmd == 'query':
            status, sendback = query_job(jdatparm)
        elif jdatcmd == 'info':
            status, sendback = node_info()
        else:
            print("bad input! cmd error!: {}".format(recv_data))
        new_client_socket.send(sendback)
        new_client_socket.close()
        if status != 0: continue


def run_job_sequency():
    while(True):
        if config.debug: print("now run_job_sequency start")
        if (config.ijob_next > config.ijob_max): # no more jobs to run
            time.sleep(5)
            continue
        job = config.jobs[config.ijob_next]
        threads = job['threads']
        if threads > config.node_max_threads:
            threads = config.node_max_threads
            print("WARN: Threads can not bigger than node!")
        node = config.find_usable_node(threads)
        if (node != None):
            if lock_jobs.acquire():
                job['status'] = 'running'
                job['run_start_time'] = time.time()
                job['node'] = node
                job['jobid'] = config.ijob_next
                #ssh_run(node, threads, cmd)
                threading.Thread(target=ssh_run,
                                 args=([job])).start()
                config.ijob_next = config.ijob_next + 1
                config.jobs_pickle_save()
            lock_jobs.release()
        else: # no usable node
            time.sleep(5)




def main():
    threading.Thread(target=init_socket).start()
    threading.Thread(target=run_job_sequency).start()

parser = argparse.ArgumentParser(description='manual to this script')
parser.add_argument( '-c', "--config", type=str, dest = "configfile",
                    default = "{}.configs.yaml".format(__file__) )
args = parser.parse_args()


config = Config(args.configfile)

if __name__ == "__main__":
    Watcher()
    main()
