#!/usr/bin/env python3
from jetlag import Universal, pp, mk_input, pcmd, RemoteJobWatcher
from knownsystems import *

from time import sleep
import os
import html
import re

# Test creation of shelob configuration using Agave
uv = Universal()
uv.init(
  backend = backend_agave2,
  notify = '{NOTIFY_URL_PASSWORD}',
  **rostam
)
os.unlink(uv.get_auth_file())
uv.init(
  backend = backend_agave2,
  notify = '{NOTIFY_URL_PASSWORD}',
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

if True: 
    j2 = RemoteJobWatcher(uv, uv.hello_world_job('queue'))
    print("Job was submitted")
    j2.wait()
    assert j2.status() == "FINISHED"
else:
    print("Test skipped: Agave can't queue on slurm")


print("Test passed")
exit(0)
