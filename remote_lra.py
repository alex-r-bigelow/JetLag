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

def lra(file_name, xlo1, xhi1, ylo1, yhi1, xlo2, xhi2, ylo2, yhi2, alpha,
        iterations, enable_output):
    from phylanx.ast import file_read_csv, constant, shape, transpose, exp, dot
    data = file_read_csv(file_name)
    x = data[xlo1:xhi1, ylo1:yhi1]
    y = data[xlo2:xhi2, ylo2]
    weights = constant(0.0, shape(x, 1))
    transx = transpose(x)
    pred = constant(0.0, shape(x, 0))
    error = constant(0.0, shape(x, 0))
    gradient = constant(0.0, shape(x, 1))
    step = 0
    while step < iterations:
        if enable_output:
            print("step: ", step, ", ", weights)
        pred = 1.0 / (1.0 + exp(-dot(x, weights)))
        error = pred - y
        gradient = dot(transx, error)
        weights = weights - (alpha * gradient)
        step += 1
    return weights

job = remote_run(uv, lra, ("/phylanx-data/CSV/breast_cancer.csv", 0, 569, 0, 30, 0, 569, 30, 31, 1e-5, 750, 0), nodes=16, ppn=16)
job.wait()
print("result:",job.get_result())

try:
    viz(job)
except:
    print("Exception during viz step:",sys.exc_info()[0])
