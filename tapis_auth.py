from subprocess import Popen, PIPE
import re, json, os
from dateutil import parser
import datetime, time

def auth(user,passw,tenant,baseurl):
    print("Running TAPIS AUTH")
    os.environ["AGAVE_CACHE_DIR"]=os.environ["HOME"]+"/.agave"

    p = Popen(["tapis","auth","tokens","refresh"])
    p.communicate()
    if p.returncode != 0:
        cmd = ["tapis","auth","init","--tenant-id",tenant,"--username",user,"--password",passw]
        print(' '.join(cmd))
        p2 = Popen(cmd)
        p2.communicate()
        if p2.returncode != 0:
            raise Exception("Could not create credentials")
