#!/usr/bin/env python3
import os
from os.path import dirname
import re
import math
import argparse
import subprocess as sp
from sys import stderr
import pandas as pd
from io import StringIO
import socket
import json

ZZSERVER_IP = '127.0.0.1'
ZZSERVER_PORT = 23456



from snakemake import io
from snakemake.io import Wildcards
from snakemake.utils import SequenceFormatter
from snakemake.utils import AlwaysQuotedFormatter
from snakemake.utils import QuotedFormatter
from snakemake.exceptions import WorkflowError
from snakemake.logging import logger

def parse_jobscript():
    """Minimal CLI to require/only accept single positional argument."""
    p = argparse.ArgumentParser(description="SLURM snakemake submit script")
    p.add_argument("jobscript", help="Snakemake jobscript with job properties.")
    return p.parse_args().jobscript


def parse_sbatch_defaults(parsed):
    """Unpack SBATCH_DEFAULTS."""
    d = parsed.split() if type(parsed) == str else parsed
    args = {}
    for keyval in [a.split("=") for a in d]:
        k = keyval[0].strip().strip("-")
        v = keyval[1].strip() if len(keyval) == 2 else None
        args[k] = v
    return args


def load_cluster_config(path):
    """Load config to dict

    Load configuration to dict either from absolute path or relative
    to profile dir.
    """
    if path:
        path = os.path.join(dirname(__file__), os.path.expandvars(path))
        dcc = io.load_configfile(path)
    else:
        dcc = {}
    if "__default__" not in dcc:
        dcc["__default__"] = {}
    return dcc


# adapted from format function in snakemake.utils
def format(_pattern, _quote_all=False, **kwargs):  # noqa: A001
    """Format a pattern in Snakemake style.
    This means that keywords embedded in braces are replaced by any variable
    values that are available in the current namespace.
    """
    fmt = SequenceFormatter(separator=" ")
    if _quote_all:
        fmt.element_formatter = AlwaysQuotedFormatter()
    else:
        fmt.element_formatter = QuotedFormatter()
    try:
        return fmt.format(_pattern, **kwargs)
    except KeyError as ex:
        raise NameError(
            f"The name {ex} is unknown in this context. Please "
            "make sure that you defined that variable. "
            "Also note that braces not used for variable access "
            "have to be escaped by repeating them "
        )


#  adapted from Job.format_wildcards in snakemake.jobs
def format_wildcards(string, job_properties):
    """ Format a string with variables from the job. """

    class Job(object):
        def __init__(self, job_properties):
            for key in job_properties:
                setattr(self, key, job_properties[key])

    job = Job(job_properties)
    if "params" in job_properties:
        job._format_params = Wildcards(fromdict=job_properties["params"])
    else:
        job._format_params = None
    if "wildcards" in job_properties:
        job._format_wildcards = Wildcards(fromdict=job_properties["wildcards"])
    else:
        job._format_wildcards = None
    _variables = dict()
    _variables.update(
        dict(params=job._format_params, wildcards=job._format_wildcards)
    )
    if hasattr(job, "rule"):
        _variables.update(dict(rule=job.rule))
    try:
        return format(string, **_variables)
    except NameError as ex:
        raise WorkflowError(
            "NameError with group job {}: {}".format(job.jobid, str(ex))
        )
    except IndexError as ex:
        raise WorkflowError(
            "IndexError with group job {}: {}".format(job.jobid, str(ex))
        )


# adapted from ClusterExecutor.cluster_params function in snakemake.executor
def format_values(dictionary, job_properties):
    formatted = dictionary.copy()
    for key, value in list(formatted.items()):
        if isinstance(value, str):
            try:
                formatted[key] = format_wildcards(value, job_properties)
            except NameError as e:
                msg = "Failed to format cluster config " "entry for job {}.".format(
                    job_properties["rule"]
                )
                raise WorkflowError(msg, e)
    return formatted


def convert_job_properties(job_properties, resource_mapping=None):
    options = {}
    if resource_mapping is None:
        resource_mapping = {}
    resources = job_properties.get("resources", {})
    if "threads" in job_properties:
        options["cpus-per-task"] = job_properties["threads"]
    for k, v in resource_mapping.items():
        options.update({k: resources[i] for i in v if i in resources})
    return options


def ensure_dirs_exist(path):
    """Ensure output folder for Slurm log files exist."""
    di = dirname(path)
    if di == "":
        return
    if not os.path.exists(di):
        os.makedirs(di, exist_ok=True)
    return



def submit_job_cmd(cmd, threads, pwd = os.getcwd(), ip=ZZSERVER_IP, port=ZZSERVER_PORT):
    """Submit jobscript and return jobid."""
    issubmit=0
    send_data = ["submit", [{'threads':threads, "pwd":pwd}, cmd]]
    send_data = json.dumps(send_data)
    try:
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_addr = (ip, port)
        tcp_socket.connect(server_addr)
        tcp_socket.send(send_data.encode("utf-8"))
        recv = tcp_socket.recv(1024).decode()
        tcp_socket.close()
        jobid = re.search(r"OK:(\d+)", recv).group(1)
        #print(jobid)
        issubmit=1
    except:
        issubmit=0
    if issubmit==1:
        return jobid
    else:
        raise ZeroDivisionError("Submit failed")


def submit_job(jobscript, **sbatch_options):
    """Submit jobscript and return jobid."""
    pwd = os.getcwd()
    cmd = "sh {}".format(jobscript)
    try:
        threads = sbatch_options['cpus-per-task']
    except:
        threads=1
    jobid = submit_job_cmd(cmd, threads, pwd)
    return jobid

