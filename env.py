#!/usr/bin/python3
import sys, os, re

# https://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
def which(program):
    import os
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    raise Exception("No such program: '%s'" % program)

for i in range(1,len(sys.argv)):
    a = sys.argv[i]
    g = re.match(r'(\w+)(%?=)(.*)', a)
    if g:
        k, eq, v = g.group(1), g.group(2), g.group(3)
        if eq == "%=":
            nv = ""
            nvs = 0
            for g in re.finditer(r'%(\d+)', v):
                nv += v[nvs:g.start()]
                nv += chr(int(g.group(1)))
                nvs = g.end()
            nv += v[nvs:]
            v = nv
        os.environ[k] = v
        if k == "PWD":
            os.chdir(v)
    else:
        os.execv(which(a),sys.argv[i:])
