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
from phylanx import Phylanx, PhylanxSession
import codecs, pickle, re, os, sys
import numpy as np

cpus = int(os.environ["CPUS"].strip())
PhylanxSession.init(16)

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

from subprocess import Popen, PIPE
use_mpi = True
cmd = []
if use_mpi:
    cmd += ["mpirun","-np",str(np)]
    #"-machinefile",machf,
else:
    cmd += ["hpxrun.py","-l",str(np)]
cmd += [os.environ["PHYSL_EXE"]]
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
