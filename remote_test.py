from remote_run import remote_run, to_string, viz
from jetlag import Universal, RemoteJobWatcher
from knownsystems import *

uv = Universal()
uv.load(
    backend_agave,
    "sbrandt@cct.lsu.edu",
    'rostam'
)

def fib(n):
    if n < 2:
        return n
    else:
        return fib(n-1)+fib(n-2)

job = remote_run(uv, fib, (13,), nodes=1, ppn=1)
job.wait()
print("result:",job.get_result())

viz(job)
