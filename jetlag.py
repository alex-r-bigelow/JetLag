####
# The basic concept of universal is:
# (1) to use either Tapis or Agave
# (2) to describe a single machine that is:
#    (a) a storage machine
#    (b) an execution machine with FORK
#    (c) an execution machine with some scheduler
# (3) Has a generic app which
#    (a) takes input.tgz
#    (b) unpacks and executes run_dir/runapp.sh
#        from inside the run_dir/ directory
#    (c) packs everything up into output.tgz
#
#####

import requests
import pprint
from math import log, ceil
from subprocess import Popen, PIPE
import sys
import os
import re
import json
from time import sleep, time
from random import randint
#from tapis_config import *
from copy import copy, deepcopy
from datetime import datetime
import codecs, pickle
import importlib.machinery
from getpass import getpass

os.environ["AGAVE_JSON_PARSER"]="jq"

job_done = ["FAILED", "FINISHED", "STOPPED", "BLOCKED"]

def decode_bytes(bs):
    s = ''
    if type(bs) == bytes:
        for k in bs:
            s += chr(k)
    return s

has_color = False
if sys.stdout.isatty():
    try:
        # Attempt to import termcolor...
        from termcolor import colored
        has_color = True
    except:
        # If this fails, attempt to install it...
        try:
            from pip import main as pip_main
        except:
            try:
                from pip._internal import main as pip_main
            except:
                pass
        try:
            pip_main(["install", "--user", "termcolor"])
            has_color = True
        except:
            pass

if not has_color:
    # Don't colorize anything if
    # this isn't a tty
    def colored(a,_):
        return a

# Agave/Tapis uses all of these http status codes
# to indicate succes.
success_codes = [200, 201, 202]

pp = pprint.PrettyPrinter(indent=2)

def age(fname):
    "Compute the age of a file in seconds"
    t1 = os.path.getmtime(fname)
    t2 = time()
    return t2 - t1

last_time = time()
pause_time = 5.1
def old_pause():
    global last_time
    now = time()
    sleep_time = last_time + pause_time - now
    if sleep_time > 0:
        sleep(sleep_time)
    last_time = time()

time_array = []

pause_files = 9
pause_time = 30

def key2(a):
    return int(1e6*a[1])

def pause():
    global time_array
    home = os.environ['HOME']
    tmp_dir = home+"/tmp/times"
    if len(time_array) == 0:
        os.makedirs(tmp_dir, exist_ok=True)
        time_array = []
        for i in range(18):
            tmp_file = tmp_dir+"/t_"+str(i)
            if not os.path.exists(tmp_file):
                with open(tmp_file,"w") as fd:
                    pass
            tmp_age = os.path.getmtime(tmp_file)
            time_array += [[tmp_file,tmp_age-pause_time]]
    time_array = sorted(time_array,key=key2)
    stime = time_array[0][1]+pause_time
    now = time()
    delt = stime - now
    if delt > 0:
        sleep(delt)
    with open(time_array[0][0],"w") as fd:
        pass
    time_array[0][1] = os.path.getmtime(time_array[0][0])

last_time_array = []

def pause1():
    global last_time_array
    now = time()
    last_time_array += [now]

    nback = 3
    nsec = 10
    nmargin = 5

    if len(last_time_array) > nback:
        if now - last_time_array[-nback] < nsec:
            stime = nsec - now + last_time_array[-nback]
            sleep(stime)
    else:
        old_pause()
    if len(last_time_array) > nback + nmargin:
        last_time_array = last_time_array[-nback:]

def check(response):
    """
    Called after receiving a response from the requests library to ensure that
    an error was not received.
    """
    assert response.status_code in success_codes, str(response)+response.content.decode()

def idstr(val,max_val):
    """
    This function is used to generate a unique
    id string to append to the end of each
    job name.
    """
    assert val < max_val
    d = int(log(max_val,10))+1
    fmt = "%0" + str(d) + "d"
    return fmt % val

def pcmd(cmd,input=None,cwd=None):
    """
    Generalized pipe command with some convenient options
    """
    #print(colored(' '.join(cmd),"magenta"))
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True, cwd=cwd)
    if input is not None:
        print("send input...")
        out, err = p.communicate(input=input)
    else:
        out, err = p.communicate()
    print(colored(out,"green"),end='')
    if err != '':
        print(colored(err,"red"),end='')
    return p.returncode, out, err

# Read a file
def readf(fname):
    with open(fname,"r") as fd:
        return fd.read()

def check_data(a,b,prefix=[]):
    """
    Used to compare data sets to see if updating
    is needed. So far, only used for storage systems.
    """
    keys = set()
    keys.update(a.keys())
    keys.update(b.keys())
    err = 0
    for k in keys:
        if k in a and k not in b:
            if len(prefix)>0 and prefix[-1] == "auth":
                pass
            else:
                print("only in a:",prefix+[k],"=>",a[k])
                err += 1
        elif k in b and k not in a:
            pass #print("only in b:",k,"=>",b[k])
        elif type(a[k]) == dict and type(b[k]) == dict:
            err += check_data(a[k],b[k],prefix + [k])
        elif a[k] != b[k]:
            if len(prefix)>0 and prefix[-1] == "auth":
                pass
            else:
                print("not equal:",prefix+[k],"=>",a[k],"!=",b[k])
                err += 1
    return err

def mk_input(input_tgz):
    """
    Generate a tarball from a hash of file names/contents.
    """
    pcmd(["rm","-fr","run_dir"])
    os.mkdir("run_dir")
    for k in input_tgz.keys():
        with open("run_dir/"+k,"w") as fd:
            print(input_tgz[k].strip(),file=fd)
    pcmd(["tar","czf","input.tgz","run_dir"])

def load_input(pass_var,is_password):
    """
    Load a password either from an environment variable or a file.
    """
    if pass_var in os.environ:
        return os.environ[pass_var]

    pfname = os.environ["HOME"]+"/."+pass_var
    if os.path.exists(pfname):
        print("reading %s from %s..." % (pass_var, pfname))
        os.environ[pass_var] = readf(pfname).strip()
        return os.environ[pass_var]

    if is_password:
        os.environ[pass_var] = getpass(pass_var+": ").strip()
    else:
        os.environ[pass_var] = input(pass_var+": ").strip()

    if not os.path.exists(pfname):
        fd = os.open(pfname, os.O_CREAT|os.O_WRONLY|os.O_TRUNC, 0o0600)
        os.write(fd,os.environ[pass_var].encode('ASCII'))
        os.close(fd)

    return os.environ[pass_var]

class RemoteJobWatcher:
    def __init__(self,uv,jobid):
        self.uv = uv
        self.jobid = jobid
        self.last_status = "EMPTY"
        self.jdata = None

    def wait(self):
        s = None
        while True:
            self.uv.poll()
            n = self.status()
            if n != s:
                print(n)
                s = n
                sleep(2)
            if n in job_done:
                return

    def stop(self):
        self.uv.job_stop(self.jobid)

    def get_result(self):
        if hasattr(self,"result"):
            return self.result
        if self.status() == "FINISHED":
            jobdir="jobdata-"+self.jobid
            if not os.path.exists(jobdir):
                os.makedirs(jobdir, exist_ok=True)
                self.uv.get_file(self.jobid,"output.tgz",jobdir+"/output.tgz")
                pcmd(["tar","xf","output.tgz"],cwd=jobdir)
                if self.jdata is None:
                    self.status(self.jobid)
                outs = self.uv.show_job(self.jobid,verbose=False,recurse=False)
                for out in outs:
                    if re.match(r'.*(\.(out|err))$',out):
                        self.uv.get_file(self.jobid, out, jobdir+"/"+re.sub(r'.*\.','job.',out))
            if os.path.exists(jobdir+'/run_dir/result.py'):
              with open(jobdir+'/run_dir/result.py',"r") as fd:
                self.result = eval(fd.read().strip())
            else:
              self.result = None
            return self.result
        return None

    def full_status(self):
        return self.uv.job_status(self.jobid)

    def status(self):
        if self.last_status in job_done:
            return self.last_status
        self.jdata = self.full_status()
        self.last_status = self.jdata["status"]
        return self.last_status

class Universal:
    """
    The Universal (i.e. Tapis or Agave) submitter thingy.
    """
    def __init__(self):

        self.auth_age = 0

        # Required options with default values.
        # Values of "uknown" or -666 mean the
        # user must supply a value.
        self.values = {
          "backend" : {},
          "email" : 'unknown',
          "sys_user" : 'unknown',
          "sys_pw" : 'unknown',
          "machine_user" : '{machine_user}',
          "machine" : 'unknown',
          "domain" : "unknown",
          "port" : 22,
          "queue" : "unknown",
          "max_jobs_per_user" : -666,
          "max_jobs" : -666,
          "max_nodes" : -666,
          "scratch_dir" : "/scratch/{machine_user}",
          "work_dir" : "/work/{machine_user}",
          "home_dir" : "/home/{machine_user}",
          "root_dir" : "/",
          "max_run_time" : "unknown",
          "max_procs_per_node" : -666,
          "min_procs_per_node" : -666,
          "allocation" : "hpc_cmr",
          "app_name" : "{machine}_queue_{other}",
          "fork_app_name" : "{machine}_fork_{other}",
          "app_version" : "1.0.0",
          "deployment_path" : "new-{utype}-deployment",
          "scheduler" : "unknown",
          "custom_directives" : ''
        }


    def load(self,backend,email,machine=None):
        self.values['backend']=backend
        if machine is not None:
            self.values['machine']=machine
        self.values['email']=email
        self.set_backend()
        self.create_or_refresh_token()

        if machine is None:
            return

        rexp = r'^machine-config-(.*)-'+machine+r'$'
        m = self.get_meta(rexp)
        assert len(m) > 0, "Could not locate machine '"+machine+"'"
        g = re.match(rexp, m[0]["name"])
        self.values["other"] = g.group(1)

        self.values["storage_id"] = self.fill('{machine}-storage-{other}')
        self.values["execm_id"] = self.fill('{machine}-exec-{other}')
        self.values["forkm_id"] = self.fill('{machine}-fork-{other}')
        m = self.get_exec()
        if m is None:
            return
        mname = m["name"]
        g = re.search(r'\((.*)\)',mname)
        assert g, mname
        self.values["machine_user"] = g.group(1)
        self.mk_extra()

        self.values['sys_pw'] = backend['pass']
        self.values["domain"] = m["site"]
        self.values["queue"] = m["queues"][0]["name"]
        self.values["max_jobs_per_user"] = m["maxSystemJobsPerUser"]
        self.values["max_jobs"] = m["maxSystemJobs"]
        self.values["max_nodes"] = m["queues"][0]["maxNodes"]

        self.values["max_procs_per_node"] = m["queues"][0]["maxProcessorsPerNode"]
        if "minProcessorsPerNode" in m["queues"][0]:
            self.values["min_procs_per_node"] = m["queues"][0]["minProcessorsPerNode"]
        else:
            self.values["min_procs_per_node"] = m["queues"][0]["maxProcessorsPerNode"]
        self.values["max_run_time"] = m["queues"][0]["maxRequestedTime"]
        self.values["custom_directives"] = m["queues"][0]["customDirectives"]
        self.values["scheduler"] = m["scheduler"]
        self.values["work_dir"] = re.sub(r'/$','',m["workDir"])
        self.values["scratch_dir"] = re.sub(r'/$','',m["scratchDir"])
        self.values["port"] = m["storage"]["port"]
        #self.values = self.fill(self.values)

        # Check for missing values
        for k in self.values:
            assert self.values[k] != "unknown", "Please supply a string value for '%s'" % k
            assert self.values[k] != -666, "Please supply an integer value for '%s'" % k

    def check_values(self, values):
        values = self.fill(values)
        for k in values:
            # We can't get minProcsPerNode back from Agave/Tapis
            if k == "min_procs_per_node":
                continue
            assert k in self.values, "Missing: "+k
            assert values[k] == self.values[k],'k: '+k+' = '+str(values[k]) + " != " + str(self.values[k])

    def init(self,**kwargs):
        # Required options with default values.
        # Values of "uknown" or -666 mean the
        # user must supply a value.
        assert "backend" in kwargs.keys(), "backend is required"
        self.values["backend"] = kwargs["backend"]

        machine_meta = {}
        for k in kwargs:
            if k not in ["email", "backend"]:
                machine_meta[k] = kwargs[k]

        self.set_backend()

        # Check the values supplied in kwargs are
        # of the expected name and type
        for k in kwargs.keys():
            assert k in self.values.keys(),\
                "Invalid argument '%s'" % k
            assert type(self.values[k]) == type(kwargs[k]),\
                "The type of arg '%s' should be '%s'" % (k, str(type(self.values[k])))
            self.values[k] = kwargs[k]
        self.values["sys_pw"] = self.values["backend"]["pass"]
        self.values["sys_user"] = self.fill(self.values["backend"]["user"])
        # we are initializing with raw data here...
        self.values["other"] = self.values["sys_user"]
        self.create_or_refresh_token()

        # Check for missing values
        for k in self.values:
            assert self.values[k] != "unknown", "Please supply a string value for '%s'" % k
            assert self.values[k] != -666, "Please supply an integer value for '%s'" % k
        self.values["storage_id"] = self.fill('{machine}-storage-{other}')
        self.values["execm_id"] = self.fill('{machine}-exec-{other}')
        self.values["forkm_id"] = self.fill('{machine}-fork-{other}')
        self.mk_extra()

        name = "machine-config-"+self.values["other"]+"-"+self.values["machine"]
        mm = {
            "name" : name,
            "value" : machine_meta
        }
        mv = self.get_meta(name)

        # Make sure there's some data there
        if len(mv) == 0:
            mv = [{"value":""}]

        # Only update the metadata if it's
        # been modified.
        if mv[0]["value"] != mm["value"]:
            self.set_meta(mm)

    def mk_extra(self):
        # Create a few extra values
        self.jobs_dir()

        self.values['app_id'] = self.fill("{app_name}-{app_version}")
        self.values['fork_app_id'] = self.fill("{fork_app_name}-{app_version}")

        # Authenticate to Agave/Tapis
        # Use generic refresh if possible

        # Create and load ssh keys
        if not os.path.exists("uapp-key.pub"):
            r, o, e =pcmd(["ssh-keygen","-m","PEM","-t","rsa","-f","uapp-key","-P",""])
            assert r == 0

        self.public_key = readf('uapp-key.pub')
        self.private_key = readf('uapp-key')

        # Create the ssh auth structure for
        # use by storage and execution systems
        self.ssh_auth = self.fill({
            "username" : "{machine_user}",
            "publicKey" : self.public_key,
            "privateKey" : self.private_key,
            "type" : "SSHKEYS"
        })

        # Create the password auth structure for
        # use by storage and execution systems
        self.pw_auth = self.fill({
            "username" : "{machine_user}",
            "password" : "",
            "type" : "PASSWORD"
        })

        # Default to password
        self.auth = self.pw_auth

    def set_backend(self):
        backend = self.values["backend"]
        for bk in ["user", "pass", "tenant", "utype"]:
            assert bk in backend.keys(), "key '%s' is required in backend: %s" % (bk,str(backend))
        self.values["sys_user"] = sys_user = backend["user"]
        pass_var = backend["pass"]
        self.values["sys_pw_env"] = pass_var
        tenant = backend["tenant"]
        self.values["tenant"] = backend["tenant"]
        self.values["utype"] = backend["utype"]
        self.values["apiurl"] = backend["baseurl"]

    def get_auth_file(self):
        user = self.fill("{sys_user}")
        if self.values['utype'] == 'tapis':
            auth_file = os.environ["HOME"]+"/.tapis/"+user+"/current"
        else:
            auth_file = os.environ["HOME"]+"/.agave/"+user+"/current"
        return auth_file

    def getauth(self):
        auth_file = self.get_auth_file()
        auth_age = os.path.getmtime(auth_file)
        if auth_age == self.auth_age:
            return self.auth_data
        self.auth_age = auth_age
        with open(auth_file,"r") as fd:
            auth_data = json.loads(fd.read())
        self.values["apiurl"] = auth_data["baseurl"]
        self.values["authtoken"] = auth_data["access_token"]
        self.auth_data = auth_data
        return auth_data

    def getheaders(self,data=None):
        """
        We need basically the same auth headers for
        everything we do. Factor out their initialization
        to a common place.
        """
        self.getauth()
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Authorization': self.fill('Bearer {authtoken}'),
            'Connection': 'keep-alive',
            'User-Agent': 'python-requests/2.22.0',
        }
        if data is not None:
            assert type(data) == str
            headers['Content-type'] = 'application/json'
            headers['Content-Length'] = str(len(data))
        return headers

    def fill(self,s,cycle={}):
        """
        Similar to string's format method, except that we
        don't require double curly's. We simply don't replace
        things we don't recognize. Also, this can apply to
        a data structure, not just a string.
        """
        if type(s) == dict:
            ns = {}
            for k in s.keys():
                ns[k] = self.fill(s[k])
            return ns
        elif type(s) == list:
            nl = []
            for item in s:
                nl += [self.fill(item)]
            return nl
        elif type(s) == tuple:
            nl = []
            for item in s:
                nl += [self.fill(item)]
            return tuple(nl)
        elif type(s) == str:
            while True:
                done = True
                ns = ''
                li = 0
                for g in re.finditer(r'{(\w+)}',s):
                    ns += s[li:g.start(0)]
                    li = g.end(0)
                    key = g.group(1)
                    assert key not in cycle, key+":"+s

                    val = None
                    if key in self.values.keys():
                        cy = copy(cycle)
                        val = self.fill(self.values[key],cy)
                        cy[key]=1
                    else:
                        if re.match(r'.*_USER$',key):
                            val = load_input(key,False)
                        elif re.match(r'.*_PASSWORD$',key):
                            val = load_input(key,True)

                    if val is not None:
                        ns += str(val)
                        done = False
                    else:
                        ns += s[g.start(0):g.end(0)]
                ns += s[li:]
                s = ns
                if done:
                    break
            return s
        else:
            return s

    def configure_from_password(self):
        """
        Completely configure the univeral system
        starting from a password.
        """
        #if self.pw_auth["password"] == "":
        #    self.pw_auth["password"] = load_pass(self.values["machine"].upper()+"_PASSWORD")
        self.set_auth_type("PASSWORD")
        self.mk_storage(force=True)
        self.install_key()
        self.configure_from_ssh_keys()

    def configure_from_ssh_keys(self):
        """
        Completely configure the univeral system
        starting from ssh keys.
        """
        self.set_auth_type("SSHKEYS")
        self.mk_storage(force=True)
        self.mk_execution(force=True)
        self.mk_app(force=True)

    def set_auth_type(self, auth):
        """
        Determine whether we are using passw or ssh
        """
        if auth == "SSHKEYS":
            self.auth = self.ssh_auth
        elif auth == "PASSWORD":
            self.auth = self.pw_auth
        else:
            raise Exception("auth:"+str(auth))
        print("auth is now:",self.auth['type'])

    def get_storage(self):
        headers = self.getheaders()
        response = requests.get(
            self.fill('{apiurl}/systems/v2/{storage_id}'), headers=headers)
        if response.status_code == 404:
            return None
        check(response)
        json_data = response.json()
        json_data = json_data["result"]
        return json_data

    def get_exec(self):
        headers = self.getheaders()
        url = self.fill('{apiurl}/systems/v2/{execm_id}')
        response = requests.get(url, headers=headers)
        if response.status_code == 403:
            print(url)
        if response.status_code == 404:
            return None
        check(response)
        json_data = response.json()
        json_data = json_data["result"]
        return json_data

    def get_auth_type(self):
        storage = self.get_storage()
        if storage == None:
            return "PASSWORD"
        return storage["storage"]["auth"]["type"]

    ##### Storage Machine Setup
    def mk_storage(self,force=False):

        storage_id = self.values["storage_id"]

        print("STORAGE MACHINE:",storage_id)

        port = int(self.values["port"])

        storage = self.fill({
            "id" : storage_id,
            "name" : "{machine} storage ({machine_user})",
            "description" : "The {machine} computer",
            "site" : "{domain}",
            "type" : "STORAGE",
            "storage" : {
                "host" : "{machine}.{domain}",
                "port" : port,
                "protocol" : "SFTP",
                "rootDir" : "{root_dir}",
                "homeDir" : "{home_dir}",
                "auth" : self.auth
            }
        })

        if not force:
            headers = self.getheaders()
            response = requests.get(
                self.fill('{apiurl}/systems/v2/{storage_id}'), headers=headers)
            print(self.fill('{apiurl}/systems/v2/{storage_id}'))
            check(response)
            json_data = response.json()
            json_data = json_data["result"]
            storage_update = (check_data(storage, json_data) > 0)
        else:
            storage_update = True

        if storage_update or (not self.check_machine(storage_id)):
            json_storage = json.dumps(storage)
            headers = self.getheaders(json_storage)
            response = requests.post(
                self.fill('{apiurl}/systems/v2/'), headers=headers, data=json_storage)
            check(response)

            assert self.check_machine(storage_id)

    def files_list(self, dir):
        headers = self.getheaders()
        params = (('limit','100'),('offset','0'),)

        if self.values["utype"] == 'tapis':
            dir = self.values["home_dir"]+"/"+dir

        pause()
        response = requests.get(
            self.fill('{apiurl}/files/v2/listings/system/{storage_id}//'+dir), headers=headers, params=params)
        check(response)
        return response.json()["result"]

    def job_by_name(self,name):
        headers = self.getheaders()
        params = (
            ('name', name),
        )
        response = requests.get(
            self.fill('{apiurl}/jobs/v2'), headers=headers, params=params)
        check(response)
        return response.json()["result"]

    def job_cleanup(self):
        """
        Job cleanup walks through the job directory
        on the remote machine and checks to see if
        the input staging data is still needed. If
        not, it cleans it up.

        This command also displays the output of
        the job if possible.
        """
        if self.is_tapis():
            fdata = self.files_list(self.jobs_dir())
        else:
            fdata = self.files_list(self.jobs_dir())
        for f in fdata:
            if f["format"] == "folder":
                job_name = f["name"]

                if not re.match(r'[\w-]{10,}',job_name):
                    print("Invalid job name:",job_name)
                    continue

                headers = self.getheaders()
                jdata = self.job_by_name(job_name)
                if len(jdata) == 0:
                    print("Could not lookup job by name:",job_name)
                    continue
                elif len(jdata) > 1:
                    print("Multiple jobs with name:",job_name)
                    continue
                jentry = jdata[0]

                if "status" not in jentry.keys():
                    continue

                if jentry["status"] not in job_done:
                    continue

                jobid = jentry["id"]

                headers = self.getheaders()
                pause()
                pause()
                print("Deleting job data for:",job_name,jobid)
                response = requests.delete(
                    self.fill('{apiurl}/files/v2/media/system/{storage_id}/{job_dir}/'+job_name), headers=headers)
                check(response)

    def create_or_refresh_token(self):
        auth_file = self.get_auth_file()
        if os.path.exists(auth_file):
            self.auth_mtime = os.path.getmtime(auth_file)
        if not self.refresh_token():
            self.create_token()

    def create_token(self):

        if "sys_pw" not in self.values or self.values["sys_pw"] == "unknown":
            self.values["sys_pw"] = self.values["backend"]["pass"]

        self.values["sys_user"] = self.fill(self.values["sys_user"])
        self.values["sys_pw"] = self.fill(self.values["sys_pw"])
        auth = (
            self.values["sys_user"],
            self.values["sys_pw"]
        )

        while True:
            # Create a client name and search to see if it exists
            client_name = "client-"+str(randint(1,int(1e10)))
            data = {
                'clientName': client_name,
                'tier': 'Unlimited',
                'description': '',
                'callbackUrl': ''
            }
            break
            url = self.fill('{apiurl}/clients/v2/')+client_name
            response = requests.get(url, auth=auth)
            jdata = response.json()['result']
            if response.status_code in [404, 400]:
                break
            check(response)
            assert jdata["name"] == client_name

        url = self.fill('{apiurl}/clients/v2/')
        response = requests.post(url, data=data, auth=auth)
        check(response)
        jdata = response.json()["result"]
        check(response)
        c_key = jdata['consumerKey']
        c_secret = jdata['consumerSecret']

        data = {
            'grant_type':'password',
            'scope':'PRODUCTION',
            'username':self.values['sys_user'],
            'password':self.values['sys_pw']
        }
        response = requests.post(self.fill('{apiurl}/token'), data=data, auth=(c_key, c_secret))
        jdata = response.json()

        now = time()
        delt = int(jdata["expires_in"])
        ts = now + delt

        fdata = {
            "tenantid":self.values['tenant'],
            "baseurl":self.values['apiurl'],
            "apisecret":c_secret,
            "apikey":c_key,
            "username":self.values['sys_user'],
            "access_token":jdata['access_token'],
            "refresh_token":jdata["refresh_token"],
            "expires_in":delt,
            "created_at":int(now),
            "expires_at":datetime.utcfromtimestamp(ts).strftime('%c')
        }
        auth_file = self.get_auth_file()
        os.makedirs(os.path.dirname(auth_file), exist_ok=True)
        with open(auth_file,"w") as fd:
            fd.write(json.dumps(fdata))
        return fdata


    def refresh_token(self):
        """
        This is under construction (i.e. it doesn't work).
        In principle, it can refresh an agave/tapis token.
        """

        auth_file = self.get_auth_file()
        if not os.path.exists(auth_file):
            return False 
        if age(auth_file) < 30*60:
            return True

        auth_data = self.getauth()
        data = {
          'grant_type': 'refresh_token',
          'refresh_token': auth_data['refresh_token'],
          'scope': 'PRODUCTION'
        }
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
        }
        auth = (
            auth_data['apikey'],
            auth_data['apisecret']
        )
        try:
            response = requests.post(
                self.fill('{apiurl}/token'), headers=headers, data=data, auth=auth)
            check(response)
            jdata = response.json()
            auth_data["refresh_token"] = jdata["refresh_token"]
            auth_data["access_token"] = jdata["access_token"]

            now = time()
            delt = int(jdata["expires_in"])
            auth_data["expires_in"] = delt
            ts = now + delt
            auth_data["created_at"] = int(now)
            auth_data["expires_at"] = datetime.utcfromtimestamp(ts).strftime('%c')

            with open(auth_file,"w") as fd:
                print(json.dumps(auth_data),file=fd)
                print("Token refresh successful")
                return True
        except Exception as e:
            print(e)
            pass
        return False

    def check_machine(self,machine):
        """
        Checks that we can do a files list on the machine.
        This proves (or disproves) that we have auth working.
        """
        headers = self.getheaders()
        params = (('limit','5'),('offset','0'),)
        url = self.fill('{apiurl}/files/v2/listings/system/'+machine+'/')
        pause()
        response = requests.get(url, headers=headers, params=params)
        check(response)
        file_data = response.json()["result"]
        n = 0
        for file in file_data:
            print(file["name"])
            n += 1
        assert n > 1
        return True

    ##### Execution Machine Setup
    def mk_execution(self,force=False):

        execm_id = self.values["execm_id"]
        forkm_id = self.values["forkm_id"]

        print("EXECUTION MACHINE:",execm_id)

        port = int(self.values["port"])

        execm = {
            "id" : execm_id,
            "name" : "{machine} exec ({machine_user})",
            "description" : "The {machine} execution computer",
            "site" : "{domain}",
            "public" : False,
            "status" : "UP",
            "type" : "EXECUTION",
            "executionType": "HPC",
            "scheduler" : "{scheduler}",
            "environment" : None,
            "scratchDir" : "{scratch_dir}",
            "workDir" : "{work_dir}",
            "login" : {
                "auth" : self.auth,
                "host" : "{machine}.{domain}",
                "port" : port,
                "protocol" : "SSH"
            },
            "maxSystemJobs" : "{max_jobs}",
            "maxSystemJobsPerUser" : "{max_jobs_per_user}",
            "queues" : [
              {
                "name" : "{queue}",
                "default" : True,
                "maxJobs" : "{max_jobs}",
                "maxNodes" : "{max_nodes}",
                "maxProcessorsPerNode" : "{max_procs_per_node}",
                "minProcessorsPerNode" : "{min_procs_per_node}",
                "maxRequestedTime" : "{max_run_time}"
              }
            ],
            "storage" : {
                "host" : "{machine}.{domain}",
                "port" : port,
                "protocol" : "SFTP",
                "rootDir" : "{root_dir}",
                "homeDir" : "{home_dir}",
                "auth" : self.auth
            }
        }

        forkm = copy(execm)
        forkm["id"] = forkm_id
        forkm["scheduler"] = "FORK"
        forkm["executionType"] = "CLI"

        if self.values["custom_directives"] is not None:
            for q in execm["queues"]:
                q["customDirectives"] = self.values["custom_directives"]

        assert execm["scheduler"] != "FORK"

        if force or not self.check_machine(execm_id):
            json_execm = json.dumps(self.fill(execm))
            with open("execm.txt","w") as fdw:
                fdw.write(json_execm)
            headers = self.getheaders(json_execm)
            response = requests.post(
                self.fill('{apiurl}/systems/v2/'), headers=headers, data=json_execm)
            check(response)
            assert self.check_machine(execm_id)

        if force or not self.check_machine(forkm_id):
            json_forkm = json.dumps(self.fill(forkm))
            headers = self.getheaders(json_forkm)
            response = requests.post(
                self.fill('{apiurl}/systems/v2/'), headers=headers, data=json_forkm)
            check(response)
            assert self.check_machine(forkm_id)

    def make_dir(self, dir_name):
        """
        Create a directory relative to the home dir on the remote machine.
        """
        dir_name = self.fill(dir_name)
        if self.values["utype"] == 'tapis':
            data = self.fill(json.dumps({"action": "mkdir", "path": "{home_dir}/"+dir_name}))
        else:
            data = self.fill(json.dumps({"action": "mkdir", "path": dir_name}))
        headers = self.getheaders(data)
        pause()
        response = requests.put(
            self.fill('{apiurl}/files/v2/media/system/{storage_id}/'), headers=headers, data=data)
        check(response)

    def file_upload(self, dir_name, file_name, file_contents=None):
        """
        Upload a file to a directory. The variable dir_name is relative
        to the home directory.
        """
        dir_name = self.fill(dir_name)
        file_name = self.fill(file_name)
        file_contents = self.fill(file_contents)
        if file_contents is None:
            with open(file_name, "rb") as fd:
                file_contents = fd.read()
        headers = self.getheaders()
        files = self.fill({
            'fileToUpload': (file_name, file_contents)
        })
        if self.values["utype"] == 'tapis':
            url = self.fill('{apiurl}/files/v2/media/system/{storage_id}//{home_dir}/'+dir_name)
        else:
            url = self.fill('{apiurl}/files/v2/media/system/{storage_id}/'+dir_name)
        pause()
        pause()
        response = requests.post(url, headers=headers, files=files)
        check(response)

    ###### Application Setup ###
    def mk_app(self,force=True):

        wrapper = """#!/bin/bash
        handle_trap() {
            rc=$?
            set +x
            if [ "$rc" != 0 ]
            then
              true # this command does nothing
              $(${AGAVE_JOB_CALLBACK_FAILURE})
            fi
            echo "EXIT($rc)" > run_dir/return_code.txt
            tar czf output.tgz run_dir
        }
        trap handle_trap ERR EXIT
        set -ex

        tar xzvf input.tgz
        echo "export AGAVE_JOB_NODE_COUNT=${AGAVE_JOB_NODE_COUNT}" > .env
        echo "export AGAVE_JOB_PROCESSORS_PER_NODE=${AGAVE_JOB_PROCESSORS_PER_NODE}" >> .env
        echo "export nx=${nx}" >> .env
        echo "export ny=${ny}" >> .env
        echo "export nz=${nz}" >> .env
        (cd ./run_dir && source ./runapp.sh)
        """

        app_name = self.values["app_name"]
        app_version = self.values["app_version"]
        wrapper_file = app_name + "-wrapper.txt"
        test_file = app_name + "-test.txt"

        app_id = self.values["app_id"]
        app = {
            "name" : app_name,
            "version" : app_version,
            "label" : app_name,
            "shortDescription" : app_name,
            "longDescription" : app_name,
            "deploymentSystem" : "{storage_id}",
            "deploymentPath" : "{deployment_path}",
            "templatePath" : wrapper_file,
            "testPath" : test_file,
            "executionSystem" : "{execm_id}",
            "executionType" : "HPC",
            "parallelism" : "PARALLEL",
            "allocation": "{allocation}",
            "modules":[],
            "inputs":[
                {   
                    "id":"input tarball",
                    "details":{  
                        "label":"input tarball",
                        "description":"",
                        "argument":None,
                        "showArgument":False
                    },
                    "value":{  
                        "default":"",
                        "order":0,
                        "required":False,
                        "validator":"",
                        "visible":True
                    }
                }   
            ],
            "parameters":[
                {
                  "id": "simagename",
                  "value": {
                    "visible": True,
                    "required": False,
                    "type": "string",
                    "order": 0,
                    "enquote": False,
                    "default": "ubuntu",
                    "validator": None
                  },
                  "details": {
                    "label": "Singularity Image",
                    "description": "The Singularity image to run: swan, funwave",
                    "argument": None,
                    "showArgument": False,
                    "repeatArgument": False
                  },
                  "semantics": {
                    "minCardinality": 0,
                    "maxCardinality": 1,
                    "ontology": []
                  }
                },
                {
                  "id": "needs_props",
                  "value": {
                    "visible": True,
                    "required": False,
                    "type": "string",
                    "order": 0,
                    "enquote": False,
                    "default": "ubuntu",
                    "validator": None
                  },
                  "details": {
                    "label": "Needs Properties",
                    "description": "Properties needed before the job runs",
                    "argument": None,
                    "showArgument": False,
                    "repeatArgument": False
                  },
                  "semantics": {
                    "minCardinality": 0,
                    "maxCardinality": 1,
                    "ontology": []
                  }
                },
                {
                  "id": "sets_props",
                  "value": {
                    "visible": True,
                    "required": False,
                    "type": "string",
                    "order": 0,
                    "enquote": False,
                    "default": "ubuntu",
                    "validator": None
                  },
                  "details": {
                    "label": "Sets Properties",
                    "description": "Properties set after the job runs",
                    "argument": None,
                    "showArgument": False,
                    "repeatArgument": False
                  },
                  "semantics": {
                    "minCardinality": 0,
                    "maxCardinality": 1,
                    "ontology": []
                  }
                },
                {
                  "id": "nx",
                  "value": {
                    "visible": True,
                    "required": False,
                    "type": "number",
                    "order": 0,
                    "enquote": False,
                    "default": 0,
                    "validator": None
                  },
                  "details": {
                    "label": "NX",
                    "description": "Processors in the X direction",
                    "argument": None,
                    "showArgument": False,
                    "repeatArgument": False
                  },
                  "semantics": {
                    "minCardinality": 0,
                    "maxCardinality": 1,
                    "ontology": []
                  }
                },
                {
                  "id": "ny",
                  "value": {
                    "visible": True,
                    "required": False,
                    "type": "number",
                    "order": 0,
                    "enquote": False,
                    "default": 0,
                    "validator": None
                  },
                  "details": {
                    "label": "NY",
                    "description": "Processors in the Y direction",
                    "argument": None,
                    "showArgument": False,
                    "repeatArgument": False
                  },
                  "semantics": {
                    "minCardinality": 0,
                    "maxCardinality": 1,
                    "ontology": []
                  }
                },
                {
                  "id": "nz",
                  "value": {
                    "visible": True,
                    "required": False,
                    "type": "number",
                    "order": 0,
                    "enquote": False,
                    "default": 0,
                    "validator": None
                  },
                  "details": {
                    "label": "NZ",
                    "description": "Processors in the Z direction",
                    "argument": None,
                    "showArgument": False,
                    "repeatArgument": False
                  },
                  "semantics": {
                    "minCardinality": 0,
                    "maxCardinality": 1,
                    "ontology": []
                  }
                }
            ],
            "outputs":[  
                {  
                    "id":"Output",
                    "details":{  
                        "description":"The output",
                        "label":"tables"
                    },
                    "value":{  
                        "default":"output.tgz",
                        "validator": None
                    }
                }
            ]
        }

        forkm_id = self.values["forkm_id"]
        fork_app_name = self.values["fork_app_name"]

        fork_app_id = self.values["fork_app_id"]
        forkapp = copy(app)
        forkapp["name"] = fork_app_name
        forkapp["executionSystem"] = forkm_id
        forkapp["executionType"] = "CLI"

        app = self.fill(app)
        forkapp = self.fill(forkapp)

        self.make_dir('{deployment_path}')
        if self.is_tapis():
            self.make_dir(self.jobs_dir())
        else:
            self.make_dir(self.jobs_dir())
        data = self.fill(json.dumps({"action": "mkdir", "path": "{home_dir}/{deployment_path}"}))
        headers = self.getheaders(data)
        pause()
        response = requests.put(
            self.fill('{apiurl}/files/v2/media/system/{storage_id}/'), headers=headers, data=data)
        check(response)

        home_dir = self.values["home_dir"]

        self.file_upload('{deployment_path}',wrapper_file, wrapper)
        self.file_upload('{deployment_path}',test_file,'{wrapper_file}')

        print("make app:",app["name"])
        data = json.dumps(app)
        headers = self.getheaders(data)
        response = requests.post(self.fill('{apiurl}/apps/v2/'), headers=headers, data=data)
        check(response)

        print("make fork app:",forkapp["name"])
        data = json.dumps(forkapp)
        headers = self.getheaders(data)
        response = requests.post(self.fill('{apiurl}/apps/v2/'), headers=headers, data=data)
        check(response)

    def hello_world_job(self, jtype='fork',sets_props={},needs_props={}):
        """
        Create and send a "Hello World" job to make
        sure that the system is working.
        """
        input_tgz = {
            "runapp.sh":"""
          #!/bin/bash
          hostname
          echo This is stdout
          echo This is stderr >&2
        """.lstrip()
        }

        return self.run_job('hello-world', input_tgz, jtype=jtype, run_time="00:01:00", sets_props=sets_props, needs_props=needs_props)

    def props(self, props):
        ret = []
        for k in props:
            ret += [re.sub(r'^(property-|)','property-',k)]
        return ret

    def run_job(self, job_name, input_tgz=None, jtype='fork', nodes=0, ppn=0, run_time=None, sets_props={}, needs_props={}, nx=0, ny=0, nz=0):
        """
        Run a job. It must have a name and an input tarball. It will default
        to running in a queue, but fork can also be requested. Specifying
        the run-time is a good idea, but not required.
        """
        if ppn == 0:
            ppn = int(self.fill("{max_procs_per_node}"))
        if nodes == 0:
            nodes = ceil(nx*ny*nz/ppn)

        if nx != 0 or ny != 0 or nz != 0:
            assert nx != 0 and ny != 0 and nz != 0
            assert nx*ny*nz <= ppn*nodes

        if nodes == 0:
            nodes = 1

        max_ppn = int(self.fill("{max_procs_per_node}"))
        assert ppn <= max_ppn, '%d <= %d' % (ppn, max_ppn)
        assert ppn >= 1
        assert nodes >= 1

        sets_props = self.props(sets_props)
        needs_props = self.props(needs_props)

        for k in sets_props:
            for m in self.get_meta(k):
                print("Property '%s' is already set" % k)
                return None

        if input_tgz is not None:
            mk_input(input_tgz)

        if run_time is None:
            run_time = self.values["max_run_time"]

        digits = 10
        max_val = 9e9
        while True:
            randid = randint(1, max_val)
            jid = idstr(randid, max_val)
            tmp_job_name = job_name+"-"+jid
            status = self.job_status(tmp_job_name)
            if status is None:
                job_name = tmp_job_name
                break

        for k in sets_props:
            self.set_meta({"name":k,"value":job_name})

        url = self.fill("agave://{storage_id}/{job_dir}/"+job_name+"/")
        jobs_dir = self.jobs_dir()
        self.make_dir(jobs_dir)
        job_dir = jobs_dir+'/'+job_name+'/'
        self.make_dir(job_dir)
        self.file_upload(job_dir,"input.tgz")

        job = {
            "name":job_name,
            "appId": "{fork_app_id}",
            "batchQueue": "{queue}",
            "maxRunTime": "{max_run_time}",
            "nodeCount": nodes,
            "processorsPerNode": ppn,
            "archive": False,
            "archiveSystem": "{storage_id}",
            "inputs": {
                "input tarball": url + "input.tgz"
            },
            "parameters": {
                "sets_props":",".join(sets_props),
                "needs_props":",".join(sets_props),
                "nx":nx,
                "ny":ny,
                "nz":nz
            },
            "notifications": []
        }

        if jtype == 'fork':
            job['appId'] = '{fork_app_id}'
        elif jtype == 'queue':
            job['appId'] = '{app_id}'
        else:
            raise Exception("jtype="+jtype)
        job = self.fill(job)
        
        email = self.values["email"]

        if email is not None:
            for event in job_done:
                job["notifications"] += [
                    {
                        "url":email,
                        "event":event,
                        "persistent": True,
                        "policy": {
                            "retryStrategy": "DELAYED",
                            "retryLimit": 3,
                            "retryRate": 5,
                            "retryDelay": 5,
                            "saveOnFailure": True
                        }
                    }
                ]

        ready = True
        for k in needs_props:
            assert re.match(r'^property-\w+$',k), '"'+k+'"'
            has_k = False
            for m in self.get_meta(k):
                if m["value"] == "READY":
                    has_k = True
                    break
            if not has_k:
                ready = False
                break

        if ready:
            data = json.dumps(job)
            headers = self.getheaders(data)
            pause()
            response = requests.post(self.fill('{apiurl}/jobs/v2/'), headers=headers, data=data)
            check(response)
            rdata = response.json()
            jobid = rdata["result"]["id"]
    
            print("Job ID:", jobid)
            data = {
                "jobid":jobid,
                "needs_props":list(needs_props),
                "sets_props":list(sets_props),
                "machine":self.values["machine"]
            }
            meta = {
                "name":"jobdata-"+job_name,
                "value":data
            }
            self.set_meta(meta)
            return jobid
        else:
            data = {
                "job": job,
                "needs_props":list(needs_props),
                "sets_props":list(sets_props),
                "machine":self.values["machine"]
            }
            meta = {
                "name":"jobdata-"+job_name,
                "value":data
            }
            self.set_meta(meta)
            return job_name

    def poll(self):
        for data in self.get_meta('jobdata-.*'):
            g = re.match(r'jobdata-(.*)',data['name'])
            job_name = g.group(1)
            m = data["value"]
            if "jobid" in m:
                done = False
                success = True
                jstat = self.job_status(m["jobid"])
                if jstat is not None:
                    m["status"] = jstat["status"]
                else:
                    print("deleting missing job:",m["jobid"])
                    self.del_meta(data)
                    continue
                #print("  job:",m["jobid"],"-> status:",m["status"])
                if m["status"] == "FINISHED":
                    done = True
                    try:
                        f = self.get_file(m["jobid"],"run_dir/return_code.txt")
                    except:
                        f = b'EXIT(616)'
                    g = re.match("EXIT\((\d+)\)",f.decode())
                    rc = int(g.group(1))
                    if rc != 0:
                        success = False
                    self.set_meta(data)
                elif m["status"] in job_done:
                    done = True

                if done:
                    if success:
                        for k in m["sets_props"]:
                            pm = {
                                "name" : k,
                                "value" : "READY"
                            }
                            print("'%s' is ready" % k)
                            self.set_meta(pm)
                    else:
                        for k in m["sets_props"]:
                            for m in self.get_meta(k):
                                self.del_meta(m)

                    #print("Cleaning up...",data["value"]["jobid"])
                    headers = self.getheaders()
                    pause()
                    jdata = self.job_status(m["jobid"])
                    fname = jdata['inputs']['input tarball']
                    if self.is_tapis():
                        assert type(fname) == str, fname
                    else:
                        assert type(fname) == list, fname
                        fname = fname[0]
                    jg = re.match(r'^agave://([\w-]+)/(.*)/input\.tgz$',fname)
                    assert jg is not None, fname
                    jmach = jg.group(1)
                    jdir = jg.group(2)
                    jloc = jmach+'/'+jdir
                    print("Cleanup: ",jloc,"...",end='',flush=True,sep='')
                    pause()
                    try:
                        response = requests.delete(self.fill('{apiurl}/files/v2/media/system/')+jloc, headers=headers)
                        if response.status_code == 404:
                            print("already missing...",end='',flush=True)
                        else:
                            check(response)
                        self.del_meta(data)
                        print("done")
                    except requests.exceptions.ConnectionError as ce:
                        print("...timed out")

            elif "job" in m:
                ready = True
                for k in m["needs_props"]:
                    has_k = False
                    if k == 'property-':
                        continue
                    for pm in self.get_meta(k):
                        if pm["value"] == "READY":
                            has_k = True
                    if not has_k:
                        ready = False
                        print("  waiting for:",k)
                if ready:
                    job = m["job"]
                    job_data = json.dumps(job)
                    headers = self.getheaders(job_data)
                    pause()
                    response = requests.post(self.fill('{apiurl}/jobs/v2/'), headers=headers, data=job_data)
                    check(response)
                    rdata = response.json()
                    jobid = rdata["result"]["id"]
            
                    print("Job ID:", jobid)
                    m["jobid"] = jobid
                    del m["job"]
                    self.set_meta(data)

    def del_meta(self, data):
        headers = self.getheaders()
        uuid = data['uuid']
        response = requests.delete(
            self.fill('{apiurl}/meta/v2/data/')+uuid, headers=headers)
        check(response)

    def set_meta(self, data):
        assert "name" in data
        assert "value" in data
        n = len(data.keys())
        assert n >= 2 and n <= 3

        mlist = self.get_meta(data["name"])

        headers = self.getheaders()

        files = {
            'fileToUpload': ('meta.txt', json.dumps(data)),
        }
        
        response = requests.post(
            self.fill('{apiurl}/meta/v2/data/'),
            headers=headers, files=files)
        check(response)
        jdata = response.json()
        data["uuid"] = jdata["result"]["uuid"]

        for m in mlist:
            if data["uuid"] == m["uuid"]:
                # This doesn't happen
                continue
            self.del_meta(m)

    def get_meta(self, name):
        headers = self.getheaders()
        
        params = (
            ('q', '{"name":"'+name+'"}'),
        )
        
        response = requests.get(
            self.fill('{apiurl}/meta/v2/data'), headers=headers, params=params)
        check(response)

        result = response.json()["result"]
        result2 = []
        for r in result:
            m = {}
            for k in ["name","value","uuid"]:
                m[k] = r[k]
            result2 += [m]
        return result2


    def job_status(self, jobid):
        headers = self.getheaders()
        pause()
        response = requests.get(self.fill("{apiurl}/jobs/v2/")+jobid, headers=headers)
        if response.status_code == 404:
            return None
        check(response)
        jdata = response.json()["result"]
        return jdata

    def job_stop(self, jobid):
        data = json.dumps({ "action": "stop" })
        headers = self.getheaders(data)
        pause()
        response = requests.post(self.fill("{apiurl}/jobs/v2/")+jobid, headers=headers, data=data)
        check(response)
        return response.json()

    def job_history(self, jobid):
        headers = self.getheaders()
        pause()
        response = requests.get(self.fill("{apiurl}/jobs/v2/")+jobid+'/history/', headers=headers)
        if response.status_code == 404:
            return None
        check(response)
        jdata = response.json()["result"]
        return jdata

    def job_list(self, num):
        headers = self.getheaders()
        params = (
            ('limit',num),
        )
        pause()
        response = requests.get(self.fill("{apiurl}/jobs/v2/"), headers=headers, params=params)
        check(response)
        jdata = response.json()["result"]
        return jdata

    def wait_for_job(self,jobid):
        last_status = ""
        while True:
            # A wait is baked into the job status
            # inside the check() method.
            sleep(pause_time)
            jdata = self.job_status(jobid)
            new_status = jdata["status"]
            if new_status != last_status:
                print(jdata["status"])
                last_status = new_status
            if new_status  in job_done:
                break

    def get_file(self,jobid,fname,as_file=None):
        headers = self.getheaders()
        pause()
        response = requests.get(self.fill("{apiurl}/jobs/v2/")+jobid+"/outputs/media/"+fname, headers=headers)
        check(response)
        content = response.content
        is_binary = False
        for header in response.headers:
            if header.lower() == "content-type":
                if response.headers[header].lower() == "application/octet-stream":
                    is_binary = True
        if as_file is not None:
            if is_binary:
                fd = os.open(as_file,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o0644)
                os.write(fd,content)
                os.close(fd)
            else:
                with open(as_file,"w") as fd:
                    print(content,file=fd)
        return content

    def is_tapis(self):
        return self.values["utype"] == "tapis"

    def install_key(self):
        """
        Install the ssh key on the remote machine. This assumes we have
        password access and that a .ssh/authorized_keys file already exists.
        """
        headers = self.getheaders()
        response = requests.get(
            self.fill('{apiurl}/files/v2/media/system/{storage_id}/.ssh/authorized_keys'), headers=headers)
        auth_file = response.content.decode()
        if self.public_key not in auth_file:
            print("INSTALLING KEY...")
            new_auth_file = self.public_key.strip()+'\n'+auth_file
            self.file_upload('.ssh','authorized_keys',file_contents=new_auth_file)
            print("DONE")
        else:
            print("KEY IS INSTALLED")

    def get_app_pems(self,app='queue'):
        headers = self.getheaders()
        if app == 'fork':
            app_name = self.values["fork_app_name"]
        else:
            app_name = self.values["app_name"]
        version = self.values["app_version"]
        url = self.fill("{apiurl}/apps/v2/"+app_name+"-"+version+"/pems")
        response = requests.get(url, headers=headers)
        if response.status_code == 403:
            print("error for url:",url)
        check(response)
        jdata = response.json()
        return jdata["result"][0]["permission"]

    def system_role(self, system, user, role):
        data = json.dumps({"role":role})
        headers = self.getheaders(data)
        
        url = self.fill('{apiurl}/systems/v2/'+system+'/roles/'+user)
        response = requests.post(url, headers=headers, data=data)
        check(response)

    def apps_pems(self, app, user, pem):
        data = json.dumps({'permission': pem})
        headers = self.getheaders(data)

        url=self.fill('{apiurl}/apps/v2/'+app+'/pems/'+user)
        response = requests.post(url, headers=headers, data=data)
        check(response)

    def meta_pems(self, uuid, user, pem):
        data = json.dumps({ 'permission': pem })
        headers = self.getheaders(data)

        url=self.fill('{apiurl}/meta/v2/data/'+uuid+'/pems/'+user)
        response = requests.post(url, headers=headers, data=data)
        check(response)

    def access(self, user, allow):
        # Need to grant access to the meta data, the app, the exec machine, and the storage machine
        if allow:
            role = 'USER'
            apps_pems = 'READ_EXECUTE'
            meta_pems = 'READ'
        else:
            role = 'NONE'
            apps_pems = 'NONE'
            meta_pems = 'NONE'
        self.system_role('{execm_id}',user,role)
        self.system_role('{forkm_id}',user,role)
        self.system_role('{storage_id}',user,role)
        self.apps_pems('{app_name}-{app_version}',user,apps_pems)
        self.apps_pems('{fork_app_name}-{app_version}',user,apps_pems)
        meta_name = "machine-config-"+self.values["sys_user"]+"-"+self.values["machine"]
        for mm in self.get_meta(meta_name):
            self.meta_pems(mm['uuid'],user,meta_pems)
        if allow:
            print(self.fill("Access to {app_name} granted to user "+user))
        else:
            print(self.fill("Access to {app_name} revoked from user "+user))

    def jobs_dir(self):
        if self.values["utype"] == "tapis":
            job_dir = self.fill('tjob/{sys_user}')
        else:
            job_dir = self.fill('ajob/{sys_user}')
        self.values["job_dir"] = job_dir
        return job_dir

    def show_job(self,jobid,dir='',verbose=True,recurse=True):
        if dir == "" and verbose:
            print(colored("Output for job: "+jobid,"magenta"))

        headers = self.getheaders()
        params = ( ('limit', '100'), ('offset', '0'),)
        response = requests.get(self.fill("{apiurl}/jobs/v2/")+jobid+"/outputs/listings/"+dir, headers=headers, params=params)
        check(response)
        jdata = response.json()["result"]
        outs = []
        for fdata in jdata:
            fname = fdata["path"]
            if verbose:
                print(colored("File:","blue"),fname)
            if fdata["format"] == "folder":
                if recurse:
                    outs += self.show_job(jobid,fname,verbose)
                continue
            else:
                outs += [fname]
            if dir != '':
                continue
            g = re.match(r'.*\.(out|err)$',fname)
            if g:
                contents = self.get_file(jobid, fname)
                if verbose:
                    if g.group(1) == "out":
                        print(colored(contents,"green"),end='')
                    else:
                        print(colored(contents,"red"),end='')
        return outs

if __name__ == "__main__":
    from knownsystems import *
    uv = Universal()
    backend = sys.argv[1]
    system = sys.argv[2]
    uv.load(
        backend=backends[backend],
        email='sbrandt@cct.lsu.edu',
        machine=system)
    uv.refresh_token()
    if sys.argv[3] == "job-status":
        j1 = RemoteJobWatcher(uv, sys.argv[4])
        pp.pprint(j1.full_status())
    elif sys.argv[3] == "get-result":
        j1 = RemoteJobWatcher(uv, sys.argv[4])
        j1.get_result()
    elif sys.argv[3] == "poll":
        uv.poll()
    elif sys.argv[3] == "del-meta":
        n = 0
        d = { "uuid" : sys.argv[4] }
        uv.del_meta(d)
    elif sys.argv[3] == "meta":
        n = 0
        for m in uv.get_meta(sys.argv[4]):
            n += 1
            print(n,": ",sep='',end='')
            pp.pprint(m)
    elif sys.argv[3] == "jobs":
        if 4 < len(sys.argv):
            nj = int(sys.argv[4])
        else:
            nj = 10
        for j in uv.job_list(nj):
            pp.pprint(j)
    elif sys.argv[3] == "history":
        jobid = sys.argv[4]
        hist = uv.job_history(jobid)
        pp.pprint(hist)
    elif sys.argv[3] == 'job-name':
        jdata = uv.job_by_name(sys.argv[4])
        pp.pprint(jdata)
    elif sys.argv[3] == 'cleanup':
        uv.job_cleanup()
    elif sys.argv[3] == 'hello':
        jobid = uv.hello_world_job()
        jw = RemoteJobWatcher(uv,jobid)
        jw.wait()
    elif sys.argv[3] == 'get-file':
        jobid = sys.argv[4]
        fname = sys.argv[5]
        c = uv.get_file(jobid, fname)
        print(decode_bytes(c))
    elif sys.argv[3] == 'ssh-config':
        uv.configure_from_ssh_keys()
    elif sys.argv[3] == 'access':
        user = sys.argv[4]
        if sys.argv[5] == "True":
            tf = True
        elif sys.argv[5] == "False":
            tf = False
        else:
            assert False, "arg 5 should be True/False"
        print("Access:",user,tf)
        uv.access(user,tf)
    elif sys.argv[3] == 'mkdir':
        uv.make_dir(sys.argv[4])
    else:
        raise Exception(sys.argv[3])
