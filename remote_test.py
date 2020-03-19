from remote_run import remote_run, to_string, viz
from jetlag import Universal, RemoteJobWatcher
from knownsystems import *
import sys
from random import randint

uv = Universal()
uv.load(
    backend=backend_agave,
    notify='sbrandt@cct.lsu.edu',
    jetlag_id='rostam-sbrandt',
)

print("The complete list of valid jetlag_id's that can be used for this test:")
for sys in uv.systems():
    print(sys)
print()

def fib(n):
    if n < 2:
        return n
    else:
        return fib(n-1)+fib(n-2)

fibno = randint(13,20)
print('fib(',fibno,')=...',sep='',flush=True)

job = remote_run(uv, fib, (fibno,), nodes=1, ppn=1)
job.wait()
print("result:",job.get_result())

try:
    viz(job)
except:
    print("Exception during viz step:",sys.exc_info()[0])
