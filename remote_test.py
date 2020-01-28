from remote_run import remote_run, to_string, viz
from jetlag import Universal, RemoteJobWatcher
from knownsystems import *
import sys

uv = Universal()
uv.load(
    backend=backend_tapis,
    email="sbrandt@cct.lsu.edu",
    jetlag_id='rostam-sbrandt',
)

def fib(n):
    if n < 2:
        return n
    else:
        return fib(n-1)+fib(n-2)

job = remote_run(uv, fib, (15,), nodes=1, ppn=1)
job.wait()
print("result:",job.get_result())

try:
    viz(job)
except:
    print("Exception during viz step:",sys.exc_info()[0])
