import inspect
from datetime import datetime
from urllib.parse import quote_plus
import requests
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
    try:
        with open(pre+'/py-csv.txt','r') as fd:
            csv_data = fd.read()
    
        with open(pre+'/py-tree.txt','r') as fd:
            tree_data = fd.read()
    
        with open(pre+'/py-graph.txt','r') as fd:
            graph_data = fd.read()

        with open(pre+'/py-src.txt','r') as fd:
            py_src = fd.read()

        with open(pre+'/physl-src.txt','r') as fd:
            physl_src = fd.read()

        with open(pre+'/label.txt','r') as fd:
            label = fd.read().strip()
    except:
        print("No performance data to visualize")
        #import traceback
        #traceback.print_exc()
        return

    label += "@"+jobid

    url = base_url + '/datasets/%s' % quote_plus(label)

    response = requests.post(url, stream=True, json={
        'csv': csv_data,
        'newick': tree_data,
        'dot': graph_data,
        'physl': physl_src,
        'python': py_src
    })
    if in_notebook():
        display(HTML("<a target='the-viz' href='"+base_url+"/static/interface.html?x=%f'>%s</a>" % (random(), label)))
    else:
        print("URL:",base_url+"/static/interface.html")
    return response
