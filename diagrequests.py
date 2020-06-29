import pprint
import requests
import os
pp = pprint.PrettyPrinter(indent=4)

save = None

def all(mname, args, kargs, verbose=False):
    global save
    if "JETLAG_DEBUG" in os.environ or verbose:
        print()
        print("="*50)
        print("requests => ",mname,"(*args, **kargs)")
        print(" where ")
        print("args:")
        pp.pprint(args)
        print("kargs:")
        k = 'Authorization'
        h = kargs.get('headers',None)
        if h:
            auth = h.get(k, None)
            if auth is not None:
                h[k] = "[hidden]"
        else:
            auth = None
        pp.pprint(kargs)
        if auth is not None:
            h[k] = auth
        print("="*50)
        print()
    else:
        save = (mname, args, kargs)

def show():
    if save is not None:
        all(save[0], save[1], save[2], True)

def get(*args,**kargs):
    all("get",args,kargs)
    return requests.get(*args,**kargs)

def post(*args,**kargs):
    all("post",args,kargs)
    return requests.post(*args,**kargs)

def delete(*args,**kargs):
    all("delete",args,kargs)
    return requests.delete(*args,**kargs)

def put(*args,**kargs):
    all("put",args,kargs)
    return requests.put(*args,**kargs)
