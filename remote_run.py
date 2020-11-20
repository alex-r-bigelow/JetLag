from jetlag import Universal, pp, mk_input, pcmd, RemoteJobWatcher
from knownsystems import *
from time import sleep
import os
import sys
import html
import inspect
import codecs, pickle, re
from visualizeInTraveler import *

def to_string(obj):
    return re.sub(b'\\s',b'',codecs.encode(pickle.dumps(obj),'base64'))

def from_string(s):
    return pickle.loads(codecs.decode(s,'base64'))

def mk_label(fname, real_args):
    args = ''
    for i in range(len(real_args)):
      if i > 0:
        args += ','
      sa = str(real_args[i])
      if len(sa) > 5:
          sa = sa[0:5]+"..."
      args += str(sa)
    if len(args) > 10:
      args=args[0:10]+"..."
    return  html.escape(fname+"("+args + ")")



def viz(job,verbose=False):
    try:
      with open("run_dir/name.txt","r") as fd:
        fname = fd.read().strip()
      response = visualizeRemoteInTraveler(job.jobid,verbose=verbose)
    except Exception as e:
      print("Could not visualize result, Traveler missing/unavailable:")
      print("exception:",e)
      #import traceback
      #traceback.print_exc()

def remote_run(uv, fun, args, queue='fork', lim='00:05:00', nodes=0, ppn=0):
    if hasattr(fun, "backend"):
        wfun = fun.backend.wrapped_function
    else:
        wfun = fun
    funname = wfun.__name__
    src = inspect.getsource(wfun)
    pargs = to_string(args)
    label = mk_label(funname, args)

    input_tgz = {
      "hpxrun-jetlag.py" : open("/JetLag/hpxrun-jetlag.py", "r").read(),
      "env.py" : open("/JetLag/env.py", "r").read(),
      "filter.json" : open("/JetLag/filter.json", "r").read(),
      "py-src.txt" : src,
      "label.txt" : label,
      "name.txt" : funname,
      "runapp.sh" : """#!/bin/bash
source ../.env
export CPUS=$(lscpu | grep "^CPU(s):"|cut -d: -f2)
export APEX_OTF2=1
export APEX_PAPI_METRICS="PAPI_TOT_CYC PAPI_BR_MSP PAPI_TOT_INS PAPI_BR_INS PAPI_LD_INS PAPI_SR_INS PAPI_L1_DCM PAPI_L2_DCM"
export PYTHONUSERBASE=/usr/local/userbase
export PHYSL_EXE=/usr/local/build/bin/physl
pwd
singularity exec $SING_OPTS $JETLAG_IMAGE python3 command.py
""",
      "command.py" : """#!/usr/bin/env python3
import codecs, pickle, re, os, sys

# Need to make sure APEX is turned off
# when we generate the physl source files
# otherwise things hang.
os.environ["APEX_OTF2"]="0"

from phylanx import Phylanx, PhylanxSession
import numpy as np
import socket

# Possibly convert a host name to an ipaddress
def format_host(host, ipaddr):
    if ipaddr:
        return socket.gethostbyname(host)
    else:
        return host

# Slurm gives hostnames like: mach[01-02]
# instead of  mach01,mach02. Expand this out.
def unslurm(fname, ipaddr=False):
    hosts = []
    g = re.match(r'([\w-]+)\[([\d,-]+)\]', fname)
    if g:
        base = g.group(1)
        for ext in g.group(2).split(','):
            g2 = re.match(r'(\d+)-(\d+)', ext)
            if g2:
                assert len(g2.group(1)) == len(g2.group(2))
                fmt = "%0"+str(len(g2.group(1)))+"d"
                for i in range(int(g2.group(1)), int(g2.group(2))+1):
                    hosts += [format_host(base + (fmt % i),ipaddr)]
            else:
                hosts += [format_host(base+ext,ipaddr)]
    else:
        hosts += [format_host(fname,ipaddr)]
    return hosts

# Ignoring the batch environment is
# the most important thing. It can
# cause generation of physl source
# to hang.
cfg = [
    "hpx.run_hpx_main!=1",
    "hpx.commandline.allow_unknown!=1",
    "hpx.commandline.aliasing!=0",
    "hpx.os_threads!=1",
    "hpx.diagnostics_on_terminate!=0",
    "hpx.ignore_batch_env!=1",
]

PhylanxSession.config(cfg)

def to_string(obj):
    return re.sub(b'\\s',b'',codecs.encode(pickle.dumps(obj),'base64'))

def from_string(s):
    return pickle.loads(codecs.decode(s,'base64'))

import numpy as np
def fstr(a):
    if type(a) in [bool, float, int, str, np.int64, np.float64, np.int32, np.float32]:
        return str(a).lower()
    elif type(a) in [list,np.ndarray]:
        s = "["
        for i in range(len(a)):
            if i > 0:
                s += ","
            if i % 10 == 9:
                # Note that because this is a string, the backslash
                # n will get expanded... so double it.
                s += "\\n"
            s += fstr(a[i])
        s += "]"
        return s
    else:
        raise Exception("Unsupported type "+str(type(a)))

args = from_string({argsrc})

@Phylanx(startatlineone=True)
{funsrc}

with open("call_{funname}.physl","w") as fw:
    alist = []
    aasign = []
    for i in range(len(args)):
        argn = "a"+str(i)
        alist += [argn]
        aasign += ["define(",argn+","+fstr(args[i])+")"]
    from phylanx.ast.physl import print_physl_src
    import contextlib, io
    physl_src_raw = {funname}.get_physl_source()
    f = io.StringIO()
    with contextlib.redirect_stdout(f):
        print_physl_src(physl_src_raw)
    physl_src_pretty = f.getvalue()
    print(physl_src_raw,file=fw)

    for a in aasign:
        print(a,file=fw)
    print("{funname}("+(",".join(alist))+")",file=fw)

    np = int(os.environ["AGAVE_JOB_PROCESSORS_PER_NODE"])*int(os.environ["AGAVE_JOB_NODE_COUNT"])

    if "PBS_NODEFILE" in os.environ:
        machf = os.environ["PBS_NODEFILE"]
    elif "SLURM_JOB_NODELIST" in os.environ:
        hosts = os.environ["SLURM_JOB_NODELIST"]
        machf = "hosts.txt"
        with open(machf,"w") as fd:
            for i in range(np):
                print(hosts,file=fd)
    else:
        machf = "hosts.txt"
        with open(machf,"w") as fd:
            for i in range(np):
                print("localhost",file=fd)

from random import randint
from subprocess import Popen, PIPE
use_mpi = False
has_mpi = False
with open("/hpx/build/CMakeCache.txt", "r") as fd:
    for line in fd.readlines():
        g = re.match(r'HPX_WITH_PARCELPORT_MPI:BOOL=(\w+)',line)
        if g:
            val = g.group(1).lower()
            if val == "on":
                has_mpi = True
            elif vall == "off":
                has_mpi = False
            else:
                raise Exception("Bad MPI parcelport value")
            break
cmd = []
if use_mpi:
    cmd += ["mpirun","-np",str(np)]
    #"-machinefile",machf,
else:
    hpxrun = "./hpxrun-jetlag.py"
    port = str(randint(7900,8000))
    cmd += ["python3",hpxrun,"-d",port,"-l",str(np),"--environ=APEX_OTF2,APEX_PAPI_METRICS,APEX_EVENT_FILTER_FILE,PWD"]
    if "SLURM_NODELIST" in os.environ:
        hosts = unslurm(os.environ["SLURM_NODELIST"],True)
        print("HOSTS:",hosts)
        if len(hosts) > 1:
            cmd += ["-n",','.join(hosts)]
cmd += [os.environ.get("PHYSL_EXE","/usr/local/build/physl")]
if not use_mpi:
    cmd += ["--"]
cmd += [
    "--dump-counters=py-csv.txt",
    "--dump-newick-tree=py-tree.txt",
    "--dump-dot=py-graph.txt",
    "--performance",
    "--print=result.py",
    "call_{funname}.physl"
]
print("cmd:",' '.join(cmd))
print("cmd:",' '.join(cmd),file=sys.stderr)
os.environ["APEX_OTF2"]="1"
#os.environ["APEX_EVENT_FILTER_FILE"]="filter.json"
p = Popen(cmd,stdout=PIPE,stderr=PIPE,universal_newlines=True)
out, err = p.communicate()
print(out,end='')
print(err,end='',file=sys.stderr)

with open("physl-src.txt","w") as fd:
    print(physl_src_pretty,file=fd)

""".format(funsrc=src, funname=funname, argsrc=pargs)
    }
    jobid = uv.run_job('py-fun',input_tgz,jtype=queue,run_time=lim,nodes=nodes,ppn=ppn)
    return RemoteJobWatcher(uv,jobid)
