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

os.environ["AGAVE_JSON_PARSER"]="jq"

if sys.stdout.isatty():
    try:
        # Attempt to import termcolor...
        from termcolor import colored
    except:
        # If this fails, attempt to install it...
        try:
            from pip import main as pip_main
        except ImportError:
            from pip._internal import main as pip_main
        
        try:
            pip_main(["install", "--user", "termcolor"])
        except SystemExit as e:
            # Give up. No colors. :(
            def colored(a,_):
                return a

else:
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
def pause():
    global last_time
    now = time()
    sleep_time = last_time + pause_time - now
    if sleep_time > 0:
        #print("pause(",sleep_time,")",sep='')
        sleep(sleep_time)
    last_time = time()

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
    from math import log
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

def load_pass(pass_var):
    """
    Load a password either from an environment variable or a file.
    """
    if pass_var in os.environ:
        return os.environ[pass_var]

    pfname = os.environ["HOME"]+"/."+pass_var
    if os.path.exists(pfname):
        os.environ[pass_var] = readf(pfname).strip()
        return os.environ[pass_var]

    from getpass import getpass
    os.environ[pass_var] = getpass(pass_var+": ").strip()
    return os.environ[pass_var]

class RemoteJobWatcher:
    def __init__(self,uv,jobid):
        self.uv = uv
        self.jobid = jobid
        self.last_status = "EMPTY"

    def wait(self):
        s = None
        while True:
            self.uv.poll()
            n = self.status()
            if n != s:
                print(n)
                s = n
                sleep(2)
            if n in ["FINISHED", "BLOCKED", "FAILED"]:
                return

    def get_result(self):
        if hasattr(self,"result"):
            return self.result
        if self.status() == "FINISHED":
            if not os.path.exists(self.jobid):
                os.makedirs(self.jobid, exist_ok=True)
                self.uv.get_file(self.jobid,"output.tgz",self.jobid+"/output.tgz")
                pcmd(["tar","xvf","output.tgz"],cwd=self.jobid)
            if os.path.exists(self.jobid+'/run_dir/result.py'):
              modulename = importlib.machinery.SourceFileLoader('rr',self.jobid+'/run_dir/result.py').load_module()
              self.result = pickle.loads(codecs.decode(modulename.result,'base64'))
            else:
              self.result = None
            return self.result
        return None

    def full_status(self):
        return self.uv.job_status(self.jobid)

    def status(self):
        if self.last_status in ["FAILED","FINISHED","BLOCKED"]:
            return self.last_status
        jdata = self.uv.job_status(self.jobid)
        self.last_status = jdata["status"]
        return self.last_status

class Universal:
    """
    The Universal (i.e. Tapis or Agave) submitter thingy.
    """
    def __init__(self):

        self.auth_mtime = 0

        # Required options with default values.
        # Values of "uknown" or -666 mean the
        # user must supply a value.
        self.values = {
          "backend" : {},
          "email" : 'unknown',
          "sys_user" : 'unknown',
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
          "app_name" : "{machine}_queue_{sys_user}",
          "fork_app_name" : "{machine}_fork_{sys_user}",
          "app_version" : "1.0.0",
          "deployment_path" : "new-{utype}-deployment",
          "scheduler" : "unknown",
          "custom_directives" : ''
        }


    def load(self,backend,email,machine):
        self.values['backend']=backend
        self.values['machine']=machine
        self.values['email']=email
        self.set_backend()
        self.create_or_refresh_token()

        self.values["storage_id"] = self.fill('{machine}-storage-{sys_user}')
        self.values["execm_id"] = self.fill('{machine}-exec-{sys_user}')
        self.values["forkm_id"] = self.fill('{machine}-fork-{sys_user}')
        m = self.get_exec()
        mname = m["name"]
        g = re.search(r'\((.*)\)',mname)
        assert g, mname
        self.values["machine_user"] = g.group(1)
        self.mk_extra()

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
        self.values = self.fill(self.values)

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
        self.values = self.fill(self.values)
        self.create_or_refresh_token()

        # Check for missing values
        for k in self.values:
            assert self.values[k] != "unknown", "Please supply a string value for '%s'" % k
            assert self.values[k] != -666, "Please supply an integer value for '%s'" % k
        self.mk_extra()

        mm = {
            "name" : "system-config-"+self.values["sys_user"]+"-"+self.values["machine"],
            "value" : machine_meta
        }
        self.set_meta(mm)

    def mk_extra(self):
        # Create a few extra values
        self.values["storage_id"] = self.fill('{machine}-storage-{sys_user}')
        self.values["execm_id"] = self.fill('{machine}-exec-{sys_user}')
        self.values["forkm_id"] = self.fill('{machine}-fork-{sys_user}')
        if self.values["utype"] == "tapis":
            self.values["job_dir"] = self.fill('{home_dir}/tjob')
        else:
            self.values["job_dir"] = self.fill('ajob')

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
        #sys_pw = load_pass(pass_var)
        self.values["sys_pw_env"] = pass_var
        tenant = backend["tenant"]
        self.values["tenant"] = backend["tenant"]
        self.values["utype"] = backend["utype"]
        self.values["apiurl"] = backend["baseurl"]

    def get_auth_file(self):
        if self.values['utype'] == 'tapis':
            auth_file = os.environ["HOME"]+"/.agave/current"
        else:
            auth_file = os.environ["HOME"]+"/.agave1/current"
        return auth_file

    def getauth(self):
        auth_file = self.get_auth_file()
        with open(auth_file,"r") as fd:
            auth_data = json.loads(fd.read())
        self.values["apiurl"] = auth_data["baseurl"]
        self.values["authtoken"] = auth_data["access_token"]
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
                    if key in self.values.keys():
                        cy = copy(cycle)
                        cy[key]=1
                        val = self.fill(self.values[key],cy)
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
        if self.pw_auth["password"] == "":
            self.pw_auth["password"] = load_pass(self.values["machine"].upper()+"_PASSWORD")
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
        response = requests.get(
            self.fill('{apiurl}/systems/v2/{execm_id}'), headers=headers)
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
        print("storage update:",storage_update)

        if storage_update or (not self.check_machine(storage_id)):
            print("Updating",storage_id,"...")
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
            fdata = self.files_list("tjob")
        else:
            fdata = self.files_list("ajob")
        for f in fdata:
            if f["format"] == "folder":
                job_name = f["name"]
                headers = self.getheaders()
                params = (
                    ('pretty', 'true'),
                    ('limit', '1'),
                    ('filter', 'id,name'),
                    ('name.like', job_name),
                    ('owner.eq', self.values['sys_user']),
                )
                
                response = requests.get(
                    self.fill('{apiurl}/jobs/v2'), headers=headers, params=params)
                check(response)
                jdata = response.json()["result"]

                for jentry in jdata:
                    if "id" not in jentry.keys():
                        continue
                    jobid = jentry["id"]
    
                    jstat = self.job_status(jobid)
                    if jstat is None:
                        continue
    
                    if jstat["status"] not in ["FINISHED", "FAILED", "BLOCKED"]:
                        continue

                    #try:
                    #    self.show_job(jobid)
                    #except:
                    #    print("Job:",jobid,"cannot be displayed")
    
                    headers = self.getheaders()
                    for tri in range(3):
                        try:
                            response = requests.delete(
                                self.fill('{apiurl}/files/v2/media/system/{storage_id}/{job_dir}/'+f["name"]), headers=headers)
                            break
                        except:
                            sleep(pause_time)

                    check(response)

    def create_or_refresh_token(self):
        auth_file = self.get_auth_file()
        if os.path.exists(auth_file):
            self.auth_mtime = os.path.getmtime(auth_file)
        if not self.refresh_token():
            self.create_token()

    def create_token(self):
        #self.values["sys_user"] = backend['user']
        #self.values["utype"] = backend['utype']
        #self.values["tenant"] = backend['tenant']
        #self.values["sys_pw"] = load_pass(backend['pass'])
        #self.values["apiurl"] = backend["baseurl"]

        if "sys_pw" not in self.values:
            self.values["sys_pw"] = load_pass(self.values["sys_pw_env"])

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
            print("refresh...")
            response = requests.post(
                self.fill('{apiurl}/token'), headers=headers, data=data, auth=auth)
            print("refresh done")
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
        #print("uploading:",file_name,"to",dir_name)
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
        response = requests.post(url, headers=headers, files=files)
        check(response)

    ###### Application Setup ###
    def mk_app(self,force=True):

        wrapper = """#!/bin/bash
        handle_trap() {
            rc=$?
            if [ "$rc" != 0 ]
            then
              true # this command does nothing
              $(${AGAVE_JOB_CALLBACK_FAILURE})
            fi
            tar czf output.tgz run_dir
            echo "EXIT($rc)" > run_dir/return_code.txt
        }
        trap handle_trap ERR EXIT
        set -ex

        tar xzvf input.tgz
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
            self.make_dir('tjob')
        else:
            self.make_dir('ajob')
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

        return self.run_job('hello-world', input_tgz, jtype, "00:01:00", sets_props, needs_props)

    def props(self, props):
        ret = []
        for k in props:
            ret += [re.sub(r'^(property-|)','property-',k)]
        return ret

    def run_job(self, job_name, input_tgz, jtype='fork', run_time = None, sets_props = {}, needs_props = {}):
        """
        Run a job. It must have a name and an input tarball. It will default
        to running in a queue, but fork can also be requested. Specifying
        the run-time is a good idea, but not required.
        """
        sets_props = self.props(sets_props)
        needs_props = self.props(needs_props)

        for k in sets_props:
            for m in self.get_meta(k):
                print("Property '%s' is already set" % k)
                return None

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
        if self.is_tapis():
            self.make_dir('tjob')
            job_dir = 'tjob/'+job_name+'/'
        else:
            self.make_dir('ajob')
            job_dir = 'ajob/'+job_name+'/'
        self.make_dir(job_dir)
        self.file_upload(job_dir,"input.tgz")

        job = {
            "name":job_name,
            "appId": "{fork_app_id}",
            "batchQueue": "{queue}",
            "maxRunTime": "{max_run_time}",
            "nodeCount": 1,
            "processorsPerNode": int(self.fill("{max_procs_per_node}")),
            "archive": False,
            "archiveSystem": "{storage_id}",
            "inputs": {
                "input tarball": url + "input.tgz"
            },
            "parameters": {
                "sets_props":",".join(sets_props),
                "needs_props":",".join(sets_props),
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
            for event in ["FAILED", "FINISHED"]:
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
        for data in self.get_meta('jobdata-*'):
            g = re.match(r'jobdata-(.*)',data['name'])
            job_name = g.group(1)
            m = data["value"]
            if "machine" not in m:
                mach = m['job']['archiveSystem']
                g = re.match(r'(.*)-storage-(.*)',mach)
                m["machine"] = g.group(1)
            if m["machine"] != self.values["machine"]:
                print("skipping",m["machine"],"!=",self.values["machine"])
                continue
            if "jobid" in m:
                done = False
                success = True
                #m["status"] = self.job_status(m["jobid"])["status"]
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
                elif m["status"] in ["FAILED", "BLOCKED"]:
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
                    response = requests.delete(
                        self.fill('{apiurl}/files/v2/media/system/{storage_id}/{job_dir}/')+job_name, headers=headers)
                    #check(response)
                    self.del_meta(data)
                    #print("  ...done")

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
            if new_status  in ["FAILED", "FINISHED", "BLOCKED"]:
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

    def show_job(self,jobid,dir=''):
        if dir == "":
            print(colored("Output for job: "+jobid,"magenta"))

        headers = self.getheaders()
        params = ( ('limit', '100'), ('offset', '0'),)
        response = requests.get(self.fill("{apiurl}/jobs/v2/")+jobid+"/outputs/listings/"+dir, headers=headers, params=params)
        check(response)
        jdata = response.json()["result"]
        for fdata in jdata:
            fname = fdata["path"]
            print(colored("File:","blue"),fname)
            if fdata["format"] == "folder":
                self.show_job(jobid,fname)
                continue
            if dir != '':
                continue
            g = re.match(r'.*\.(out|err)$',fname)
            if g:
                contents = self.get_file(jobid, fname)
                if g.group(1) == "out":
                    print(colored(contents,"green"),end='')
                else:
                    print(colored(contents,"red"),end='')

if __name__ == "__main__":
    from knownsystems import *
    uv = Universal()
    backend = sys.argv[1]
    system = sys.argv[2]
    uv.init(
        backend=backends[backend],
        email='sbrandt@cct.lsu.edu',
        **systems[system])
    uv.refresh_token()
    if sys.argv[3] == "job-status":
        j1 = RemoteJobWatcher(uv, sys.argv[4])
        pp.pprint(j1.full_status())
    elif sys.argv[3] == "poll":
        uv.poll()