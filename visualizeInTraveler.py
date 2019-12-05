import inspect
from datetime import datetime
from urllib.parse import quote_plus
import requests
from IPython.core.display import display, HTML
from TwoWayWebView import TwoWayWebView
from random import random

base_url = "http://localhost:8100"

def visualizeInTraveler(fun):
    widget = TwoWayWebView(filename='uploadWidget.html')
    display(widget)

    label = 'Jupyter@' + datetime.now().isoformat()
    widget.sendObject({'datasetLabel': label})
    url = base_url+'/datasets/%s' % quote_plus(label)
    response = requests.post(url, stream=True, json={
        'csv': fun.__perfdata__[0],
        'newick': fun.__perfdata__[1],
        'dot': fun.__perfdata__[2],
        'physl': fun.__src__,
        'python': inspect.getsource(fun.backend.wrapped_function)
    })
    for line in response.iter_lines(decode_unicode=True):
        widget.sendObject({'messageChunk': line})
    widget.sendObject({'done': True})
    return response

def visualizeRemoteInTraveler(jobid):

    try:
        with open(jobid+'/run_dir/py-csv.txt','r') as fd:
            csv_data = fd.read()
    
        with open(jobid+'/run_dir/py-tree.txt','r') as fd:
            tree_data = fd.read()
    
        with open(jobid+'/run_dir/py-graph.txt','r') as fd:
            graph_data = fd.read()

        with open(jobid+'/run_dir/py-src.txt','r') as fd:
            py_src = fd.read()

        with open(jobid+'/run_dir/physl-src.txt','r') as fd:
            physl_src = fd.read()

        with open(jobid+'/run_dir/label.txt','r') as fd:
            label = fd.read().strip()
    except:
        print("No performance data to visualize")
        import traceback
        traceback.print_exc()
        return

    widget = TwoWayWebView(filename='uploadWidget.html')

    label += "@"+jobid

    widget.sendObject({'datasetLabel': label})
    url = base_url + '/datasets/%s' % quote_plus(label)

    response = requests.post(url, stream=True, json={
        'csv': csv_data,
        'newick': tree_data,
        'dot': graph_data,
        'physl': physl_src,
        'python': py_src
    })
    for line in response.iter_lines(decode_unicode=True):
        widget.sendObject({'messageChunk': line})
    widget.sendObject({'done': True})
    #display(widget)
    display(HTML("<a target='the-viz' href='"+base_url+"/static/interface.html?x=%f'>%s</a>" % (random(), label)))
    return response
