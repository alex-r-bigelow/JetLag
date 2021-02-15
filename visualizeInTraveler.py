from random import randint
import inspect
from datetime import datetime
from urllib.parse import quote_plus
import requests
import json
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

# Allow environment variable to redirect output to something other than
# localhost; e.g. a traveler instance outside the docker container or a
# different machine
base_url = "http://%s:%d" % (os.environ.get("TRAVELER_IP", "localhost"), traveler_port)

def in_notebook():
    try:
        get_ipython().config
        return True
    except:
        return False

def parse_traveler_response(resp, verbose):
    result = resp.json()
    # TODO: can use resp.iter_content(chunk_size=None, decode_unicode=True) to
    # catch and print partial JSON while the data is bundling (instead of
    # waiting for the whole process to finish), but displaying updates in
    # Jupyter would require a fancier widget that we can update round-trip
    # style...
    if verbose:
        print(result['log'])
    return result

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

    # Note: dataset label is optional + no longer needs to be unique;
    # defaults to "Untitled dataset" if omitted;
    # '/' characters inside the label will be interpreted as parent folders
    argMap = {
        "label":  fun_id,
        "tags":   [fun_name, 'JetLag'], # Can attach any string as a tag
        "csv":    fun.__perfdata__[0],
        "newick": fun.__perfdata__[1],
        "dot":    fun.__perfdata__[2],
        "physl":  f.getvalue(),
        "python": fun.get_python_src(fun.backend.wrapped_function)
    }

    import requests
    resp = requests.post(base_url + '/datasets', json=argMap, stream=True)
    resp.raise_for_status()
    trav_id = parse_traveler_response(resp, verbose)['datasetId']

    otf2Path = 'OTF2_archive/APEX.otf2'
    if os.path.exists(otf2Path):
        # Upload the OTF2 trace separately because we want to stream its
        # contents instead of trying to load the whole thing into memory
        def iterOtf2():
            otfPipe = subprocess.Popen(['otf2-print', otf2Path], stdout=subprocess.PIPE)
            for bytesChunk in otfPipe.stdout:
                yield bytesChunk
                otfPipe.stdout.flush()
        otf2Response = requests.post(
            base_url + '/datasets/%s/otf2' % trav_id,
            stream=True,
            timeout=None,
            data=iterOtf2(),
            headers={'content-type': 'text/text'}
        )
        otf2Response.raise_for_status()
        parse_traveler_response(otf2Response, verbose)
    if in_notebook():
        display(HTML("<a target='the-viz' href='"+base_url+"/static/interface.html#%s'>Visualize %s-%d</a>" % (trav_id, fun_name, fun_id)))
    else:
        print("URL:", base_url+"/static/interface.html")


def visualizeDirInTraveler(jobid, pre, verbose=False):
    # Read any small text files that exist
    argMap = {
        'csv':    pre+'/py-csv.txt',
        'newick': pre+'/py-tree.txt',
        'dot':    pre+'/py-graph.txt',
        'physl':  pre+'/physl-src.txt',
        'python': pre+'/py-src.txt'
    }
    postData = {
        "tags":   ['Ran via JetLag']
    }
    with open(pre+'/label.txt', 'r') as fd:
        postData['label'] = label = fd.read().strip()
    for arg, path in argMap.items():
        if os.path.exists(path):
            with open(path, 'r') as fd:
                postData[arg] = fd.read()

    # Create the dataset in traveler
    mainResponse = requests.post(base_url + '/datasets', json=postData)
    mainResponse.raise_for_status()
    trav_id = parse_traveler_response(mainResponse, verbose)['datasetId']

    otf2Path = pre+'/OTF2_archive/APEX.otf2'
    if os.path.exists(otf2Path):
        # Upload the OTF2 trace separately because we want to stream its
        # contents instead of trying to load the whole thing into memory
        def iterOtf2():
            otfPipe = subprocess.Popen(['otf2-print', otf2Path], stdout=subprocess.PIPE)
            for bytesChunk in otfPipe.stdout:
                yield bytesChunk
                otfPipe.stdout.flush()
        otf2Response = requests.post(
            base_url + '/datasets/%s/otf2' % trav_id,
            stream=True,
            timeout=None,
            data=iterOtf2(),
            headers={'content-type': 'text/text'}
        )
        parse_traveler_response(otf2Response, verbose)
    else:
        otf2Response = None
    if in_notebook():
        display(HTML("<a target='the-viz' href='"+base_url+"/static/interface.html#%s'>Visualize %s</a>" % (trav_id, label)))
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

def visualizeRemoteInTraveler(jobid, verbose=False):
    pre = 'jobdata-'+jobid+'/run_dir'
    visualizeDirInTraveler(jobid, pre, verbose)
