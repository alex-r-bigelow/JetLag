import os
from phylanx import Phylanx
import subprocess as s
from visualizeInTraveler import visualizeDirInTraveler
from random import randint

def run_local(f,args,threads=1,localities=1,perf=True):
    if not hasattr(f, "backend"):
        f = Phylanx(f)
    src = f.get_physl_source()
    fun_name = f.backend.wrapped_function.__name__
    file_name = fun_name+".physl"
    with open(file_name, "w") as fd:
        print(src, file=fd)
        if len(args)==1:
            print(fun_name,"(",args[0],")",sep="",file=fd)
        else:
            print(fun_name,args,sep="",file=fd)

    # TODO: Build alternative for using hpxrun for the
    # case where MPI is not installed.
    mpi = "mpirun"
    mpi_found = False
    for path in os.environ["PATH"].split(":"):
        mpi = os.path.join(path, "mpirun")
        if os.path.exists(mpi):
            mpi_found = True
            break

    if not mpi_found and os.path.exists("/usr/lib64/openmpi/bin/mpirun"):
        mpi = "/usr/lib64/openmpi/bin/mpirun"

    if not mpi_found and os.path.exists("/usr/lib64/mpich/bin/mpirun"):
        mpi = "/usr/lib64/mpich/bin/mpirun"
    
    if perf:
        while True:
            randval = randint(1, 2<<31)
            perf_dir = "perf-%d" % randval
            job_id = "job-%d" % randval
            if not os.path.exists(perf_dir):
                break
        os.makedirs(perf_dir)

    cmd = [mpi,"-np",str(localities),"/usr/local/build/bin/physl"]
    if perf:
        cmd += [
            "--dump-counters=%s/py-csv.txt" % perf_dir,
            "--dump-newick-tree=%s/py-tree.txt" % perf_dir,
            "--dump-dot=%s/py-graph.txt" % perf_dir,
            "--performance"]
    cmd += [
        "--print=result-%d.physl" % randval,
        "--hpx:run-hpx-main",
        "--hpx:thread=%d" % threads, file_name]

    if perf:
        os.environ["APEX_OTF2"]="1"
    else:
        if "APEX_OTF2" in os.environ:
            del os.environ["APEX_OTF2"]

    apex_otf2_path = os.path.join(perf_dir, "./OTF2_archive")
    os.environ["APEX_OTF2_ARCHIVE_PATH"] = apex_otf2_path

    env = {}
    for k in os.environ:
        env[k] = os.environ[k]
    print(" ".join(cmd))
    p = s.Popen(cmd, env=env)
    if perf:
        with open("%s/label.txt" % perf_dir, "w") as fd:
            print(job_id, file=fd)
    def viz():
        nonlocal job_id, perf_dir
        visualizeDirInTraveler(job_id, perf_dir, True)
    p.viz = viz
    return p
