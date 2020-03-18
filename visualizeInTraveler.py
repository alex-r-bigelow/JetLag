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

def visualize(f,label=None):
    if label is None:
        label = f.backend.wrapped_function.__name__
    pid = str(os.getpid())
    def write_perf(f,label):
        t = f.__perfdata__
        s = f.get_python_src(f.backend.wrapped_function)
        ps = f.get_physl_source()
        dir="jobdata-"+pid+"/run_dir"
        os.makedirs(dir,exist_ok=True)
        with open(dir+"/py-csv.txt","w") as fd:
            print(t[0],end='',file=fd)
        with open(dir+"/py-tree.txt","w") as fd:
            print(t[1],end='',file=fd)
        with open(dir+"/py-graph.txt","w") as fd:
            print(t[2],end='',file=fd)
        with open(dir+"/py-src.txt","w") as fd:
            print(s,file=fd)
        with open(dir+"/physl-src.txt","w") as fd:
            print(ps,file=fd)
        with open(dir+"/label.txt","w") as fd:
            print(label,file=fd)
    write_perf(f4,"f4(2)")
    visualizeRemoteInTraveler(pid)
