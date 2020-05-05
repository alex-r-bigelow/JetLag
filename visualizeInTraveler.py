import inspect
from datetime import datetime
from urllib.parse import quote_plus
import requests
import subprocess
import os
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

def in_notebook():
    try:
        get_ipython().config
        return True
    except:
        return False

def visualizeRemoteInTraveler(jobid):
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

    otf2Path = pre+'/OTF2_archive/APEX.otf2'
    otf2Response = None
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
    else:
        print('OTF2 does not exist: %s' % otf2Path)
    if in_notebook():
        display(HTML("<a target='the-viz' href='"+base_url+"/static/interface.html?x=%f'>%s</a>" % (random(), label)))
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
