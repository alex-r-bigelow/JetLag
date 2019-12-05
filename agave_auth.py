from subprocess import Popen, PIPE
import re, json, os
from dateutil import parser
import datetime, time

def auth(user,passw,tenant,baseurl):
    print("Running AGAVE AUTH")

    cache=os.environ["HOME"]+"/.agave1"
    os.environ["AGAVE_CACHE_DIR"]=cache
    os.environ["AGAVE_TENANTS_API_BASEURL"] = baseurl
    os.makedirs(cache,exist_ok=True)

    if os.path.exists(cache+"/current"):
        p = Popen(["auth-tokens-refresh"])
        p.communicate()
        if p.returncode == 0:
            return

    p = Popen(["tenants-init","-t",tenant])
    p.communicate()

    cmd = ["clients-delete","-u",user,"-p",passw,user+"-client"]
    p = Popen(cmd)
    p.communicate()

    cmd = ["clients-create","-p",passw,"-S","-N",user+"-client","-u",user]
    p = Popen(cmd)
    p.communicate()

    cmd = ["auth-tokens-create","-u",user,"-p",passw]
    p = Popen(cmd)
    p.communicate()
