from jetlag import Universal, pp, mk_input, pcmd, RemoteJobWatcher
from knownsystems import *
from time import sleep
import os
import html
import re

# Test creation of shelob configuration using Agave
uv = Universal()
uv.init(
  backend = backend_tapis,
  #notify = "https://www.cct.lsu.edu/~sbrandt/pushbullet.php?key={PBTOK_PASSWORD}&status=${JOB_STATUS}:${JOB_ID}",
  notify='sbrandt@cct.lsu.edu',
  **rostam
)
uv.configure_from_ssh_keys()

j1 = RemoteJobWatcher(uv, uv.hello_world_job('fork'))
print("Job was submitted")
j1.wait()
assert j1.status() == "FINISHED"
err = j1.err_output()
assert re.search(r'(?m)^This is stderr', err)
out = j1.std_output()
assert re.search(r'(?m)^This is stdout', out)
    
if True: # This does not work with Agave
    j2 = RemoteJobWatcher(uv, uv.hello_world_job('queue'))
    print("Job was submitted")
    j2.wait()
    assert j2.status() == "FINISHED"
else:
    print("Test skipped: Tapis can't queue on slurm")


print("Test passed")
exit(0)
