from jetlag import Universal, pp, mk_input, pcmd
from time import sleep
import os
import html
from knownsystems import *

if False:
    uv = Universal()
    uv.init(
      backend = backend_agave,
      notify = 'sbrandt@cct.lsu.edu',
      **shelob
    )
    uv.configure_from_ssh_keys()

# Check configuration...
uv = Universal()
uv.load(backend_agave, 'sbrandt@cct.lsu.edu', 'shelob')
uv.check_values(shelob)

mm = uv.get_meta('system-config-sbrandt-shelob')
pp.pprint(mm)

uv = Universal()
uv.load(backend_agave, 'sbrandt@cct.lsu.edu', 'rostam')
uv.check_values(rostam)

uv = Universal()
uv.load(backend_tapis, 'sbrandt@cct.lsu.edu', 'shelob')
uv.check_values(shelob)

uv = Universal()
uv.load(backend_tapis, 'sbrandt@cct.lsu.edu', 'rostam')
uv.check_values(rostam)

def dm(name):
    for m in uv.get_meta('property-'+name):
        uv.del_meta(m)

if True:
    dm('a')
    dm('b')
    dm('c')

    uv.hello_world_job('fork',sets_props={'a'},needs_props={})
    uv.hello_world_job('fork',sets_props={'b'},needs_props={'a'})
    uv.hello_world_job('fork',sets_props={'c'},needs_props={'b'})

while True:
    mav = ""
    mbv = ""
    mcv = ""
    for ma in uv.get_meta('property-a'):
        mav = ma["value"]
    for mb in uv.get_meta('property-b'):
        mbv = mb["value"]
    for mc in uv.get_meta('property-c'):
        mcv = mc["value"]
    print("m: a=",mav," b=",mbv," c=",mcv,sep='')
    uv.poll()
    if mcv == "READY":
        assert mav == "READY"
        assert mbv == "READY"
        break

print("Test successful")
