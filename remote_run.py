from jetlag import Universal, pp, mk_input, pcmd, RemoteJobWatcher
from knownsystems import *
from time import sleep
import os
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



def viz(job):
    #import importlib.machinery
    try:
      #if os.path.exists('run_dir/result.py'):
      #  modulename = importlib.machinery.SourceFileLoader('rr','run_dir/result.py').load_module()
      #  job.result = pickle.loads(codecs.decode(modulename.result,'base64'))
      #else:
      #  job.result = None
      with open("run_dir/name.txt","r") as fd:
        fname = fd.read().strip()
      response = visualizeRemoteInTraveler(job.jobid)
    except Exception as e:
      print("exception:",e)
      import traceback
      traceback.print_exc()

def remote_run(uv, fun, args, queue='fork', lim='00:05:00'):
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
export CPUS=$(lscpu | grep "^CPU(s):"|cut -d: -f2)
singularity exec ~/images/phylanx-devenv.simg python3 command.py
""",
      "command.py" : """#!/usr/bin/env python3
from phylanx import Phylanx, PhylanxSession
import codecs, pickle, re, os

cpus = int(os.environ["CPUS"].strip())
PhylanxSession.init(16)

def to_string(obj):
    return re.sub(b'\\s',b'',codecs.encode(pickle.dumps(obj),'base64'))

def from_string(s):
    return pickle.loads(codecs.decode(s,'base64'))

args = from_string({argsrc})
print("args:",args)

@Phylanx(performance='x')
{funsrc}

result = {funname}(*args)

with open("result.py","w") as fd:
    sval = to_string(result)
    print("result=%s" % sval,file=fd)

with open("physl-src.txt","w") as fd:
    print({funname}.__src__,file=fd)

files = ['py-csv.txt','py-tree.txt','py-graph.txt']
for i in range(len(files)):
    with open(files[i],"w") as fd:
        print({funname}.__perfdata__[i],file=fd)
""".format(funsrc=src, funname=funname, argsrc=pargs)
    }
    jobid = uv.run_job('py-fun',input_tgz,queue,lim)
    return RemoteJobWatcher(uv,jobid)
