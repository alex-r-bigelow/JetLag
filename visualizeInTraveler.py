from random import randint
import inspect
from datetime import datetime
from urllib.parse import quote_plus
import requests
import subprocess
import os
import contextlib, io
from phylanx.ast.physl import print_physl_src

try:
    from IPython.core.display import display, HTML
except:
    pass
from random import random

if "TRAVELER_PORT" in os.environ:
    traveler_port = int(os.environ["TRAVELER_PORT"])
else:
    traveler_port = 8000
base_url = "http://localhost:%d" % traveler_port

def print_chunks(resp):
    for chunk in resp.iter_content():
        print(chunk.decode(),end='')

def in_notebook():
    try:
        get_ipython().config
        return True
    except:
        return False

def visualizeInTraveler(fun, verbose=False):
    fun_id = randint(0,2<<31)
    fun_name = fun.backend.wrapped_function.__name__

    if verbose:
        print("APEX_OTF2:",os.environ.get("APEX_OTF2","is not set"))
        print("APEX_PAPI_METRICS:",os.environ.get("APEX_PAPI_METRICS","is not set"))

    if not hasattr(fun,"__perfdata__"):
        print("Performance data was not collected for", fun_name)
        return

    physl_src_raw = fun.get_physl_source()
    f = io.StringIO()
    with contextlib.redirect_stdout(f):
        print_physl_src(physl_src_raw)

    argMap = {
        "csv":    fun.__perfdata__[0],
        "newick": fun.__perfdata__[1],
        "dot":    fun.__perfdata__[2],
        "physl":  f.getvalue(),
        "python": fun.get_python_src(fun.backend.wrapped_function)
    }
    import requests
    base_url = "http://localhost:8000"
    
    # This loop iterates until we've found an unused id
    while True:
        url = base_url + '/datasets/%s-%d' % (fun_name, fun_id)
        fun_id = randint(0, 2<<31)
        resp = requests.post(url, json=argMap)
        if "already exists" not in resp.content.decode():
            break
    if verbose:
        print(resp.content.decode())
        
    otf2Path = 'OTF2_archive/APEX.otf2'
    if os.path.exists(otf2Path):
        # Upload the OTF2 trace separately because we want to stream its
        # contents instead of trying to load the whole thing into memory
        def iterOtf2():
            otfPipe = subprocess.Popen(['otf2-print', otf2Path], stdout=subprocess.PIPE)
            for line in otfPipe.stdout:
                yield line
        otf2Response = requests.post(
            url + '/otf2',
            stream=True,
            data=iterOtf2(),
            headers={'content-type': 'text/text'}
        )
        if verbose:
            print_chunks(otf2Response)
    if in_notebook():
        display(HTML("<a target='the-viz' href='"+base_url+"/static/interface.html?x=%f'>Visualize %s-%d</a>" % (random(), fun_name, fun_id)))
    else:
        print("URL:", base_url+"/static/interface.html")


def visualizeRemoteInTraveler(jobid, verbose=False):
    pre = 'jobdata-'+jobid+'/run_dir'

    # The only requirement is a label
    if not os.path.exists(pre+'/label.txt'):
        raise Exception("No label provided; can't visualize performance data")
    with open(pre+'/label.txt', 'r') as fd:
        label = fd.read().strip()
    label += "@"+jobid

    # Read any small text files that exist
    argMap = {
        'csv': pre+'/py-csv.txt',
        'newick': pre+'/py-tree.txt',
        'dot': pre+'/py-graph.txt',
        'physl': pre+'/physl-src.txt',
        'python': pre+'/py-src.txt'
    }
    postData = {}
    for arg, path in argMap.items():
        if os.path.exists(path):
            with open(path, 'r') as fd:
                postData[arg] = fd.read()

    # Create the dataset in traveler
    url = base_url + '/datasets/%s' % quote_plus(label)
    mainResponse = requests.post(url, json=postData)
    if verbose:
        print_chunks(mainResponse)

    otf2Path = pre+'/OTF2_archive/APEX.otf2'
    if os.path.exists(otf2Path):
        # Upload the OTF2 trace separately because we want to stream its
        # contents instead of trying to load the whole thing into memory
        def iterOtf2():
            otfPipe = subprocess.Popen(['otf2-print', otf2Path], stdout=subprocess.PIPE)
            for line in otfPipe.stdout:
                yield line
        otf2Response = requests.post(
            url + '/otf2',
            stream=True,
            data=iterOtf2(),
            headers={'content-type': 'text/text'}
        )
        if verbose:
            print_chunks(otf2Response)
    if in_notebook():
        display(HTML("<a target='the-viz' href='"+base_url+"/static/interface.html?x=%f'>Visualize %s</a>" % (random(), label)))
    else:
        print("URL:", base_url+"/static/interface.html")
    return (mainResponse, otf2Response)

if __name__ == "__main__":
    import sys
    (m, o) = visualizeRemoteInTraveler(sys.argv[1])
    for chunk in m.iter_content():
        print(chunk.decode(), end='')
    for chunk in o.iter_content():
        print(chunk.decode(), end='')
