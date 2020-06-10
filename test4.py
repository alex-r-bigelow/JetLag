from jetlag import Universal, pp, mk_input, pcmd, RemoteJobWatcher
from knownsystems import *
import pprint
pp = pprint.PrettyPrinter(indent=4)

from time import sleep
import os
import html

# Test creation of shelob configuration using Agave
uv = Universal()
uv.init(
  backend = backend_tapis,
  #notify = "https://www.cct.lsu.edu/~sbrandt/pushbullet.php?key={PBTOK_PASSWORD}&status=${JOB_STATUS}:${JOB_ID}",
  notify='sbrandt@cct.lsu.edu',
  **shelob
)
uv.configure_from_ssh_keys()

j1 = RemoteJobWatcher(uv, uv.hello_world_job('fork'))
print("Job was submitted")
j1.wait()
assert j1.status() == "FINISHED"

j2 = RemoteJobWatcher(uv, uv.hello_world_job('queue'))
print("Job was submitted")
j2.wait()
if j2.status() != "FINISHED":
    pp.pprint(j2.full_status())
    assert False

print("Test passed")
exit(0)
