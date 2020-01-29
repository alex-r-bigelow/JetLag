from jetlag import Universal, pp, mk_input, pcmd, RemoteJobWatcher
from knownsystems import *

from time import sleep
import os
import html

# Test creation of shelob configuration using Agave
uv = Universal()
uv.init(
  backend = backend_agave,
  notify = 'sbrandt@cct.lsu.edu',
  **rostam
)
uv.configure_from_ssh_keys()

j1 = RemoteJobWatcher(uv, uv.hello_world_job('fork'))
print("Job was submitted")
j1.wait()
assert j1.status() == "FINISHED"

if False: # This does not work with Agave
    j2 = RemoteJobWatcher(uv, uv.hello_world_job('queue'))
    print("Job was submitted")
    j2.wait()
    assert j2.status() == "FINISHED"
else:
    print("Test skipped: Agave can't queue on slurm")


print("Test passed")
exit(0)
