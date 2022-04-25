#!/usr/bin/env python3
import re
import subprocess as sp
import shlex
import sys
import time
import logging
import socket


ZZSERVER_IP = '127.0.0.1'
ZZSERVER_PORT = 23456


logger = logging.getLogger("__name__")

STATUS_ATTEMPTS = 20

jobid = sys.argv[1]

  # noqa: E225
cluster = ""
  # noqa: E225

issubmit=0
for i in range(STATUS_ATTEMPTS):
    try:
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_addr = (ZZSERVER_IP, ZZSERVER_PORT)
        tcp_socket.connect(server_addr)
        tcp_socket.send("[\"query\",{}]".format(jobid).encode("utf-8"))
        recv = tcp_socket.recv(1024).decode()
        tcp_socket.close()
        recv = re.search(r"^(\w+)", recv).group(1)
        issubmit=1
        break
    except:
        time.sleep(1)
        continue

if issubmit==0:
    print('running') # 缓兵之计

status = recv

if status == "Bad_JobID":
    print("failed")
elif status == "done":
    print("success")
elif status == "fail":
    print("failed")
elif status == "running":
    print("running")
elif status == "pending":
    print("running")
else:
    print("failed")
