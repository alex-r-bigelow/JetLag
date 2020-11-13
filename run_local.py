import os
from phylanx import Phylanx
import subprocess as s
from visualizeInTraveler import visualizeDirInTraveler
from random import randint
from IPython.core.display import display, HTML
import contextlib, io
from phylanx.ast.physl import print_physl_src
import html

# https://stackoverflow.com/questions/377017/test-if-executable-exists-in-python
def which(program):
    import os
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

def prettify_physl(physl_src_raw):
    iof = io.StringIO()
    with contextlib.redirect_stdout(iof):
        print_physl_src(physl_src_raw)
    return iof.getvalue()

def run_local(f,args,threads=1,localities=1,perf=True,apex=True):
    if not hasattr(f, "backend"):
        f = Phylanx(f)

    # Create source file
    src = f.get_physl_source()
    fun_name = f.backend.wrapped_function.__name__
    file_name = fun_name+".physl"
    with open(file_name, "w") as fd:
        print(src, file=fd)
        if len(args)==1:
            print(fun_name,"(",args[0],")",sep="",file=fd)
        else:
            print(fun_name,args,sep="",file=fd)

    my_dir = os.path.abspath(os.path.join(__file__, os.pardir))
    runner = os.path.join(my_dir, "hpxrun-jetlag.py")

    cmd = [runner, "-l", str(localities)]

    # Pick a random point to avoid shutdown problems
    port = randint(7910,8100)
    cmd += ["-d", str(port)]

    # Always export the APEX environment
    cmd += ["--environ=APEX_OTF2,APEX_PAPI_METRICS,APEX_EVENT_FILTER_FILE,APEX_OTF2_ARCHIVE_PATH"]
    os.environ["APEX_EVENT_FILTER_FILE"] = os.path.join(my_dir, "json.filter")
    os.environ["APEX_PAPI_METRICS"]="PAPI_TOT_CYC PAPI_BR_MSP PAPI_TOT_INS" # PAPI_BR_INS PAPI_LD_INS PAPI_SR_INS PAPI_L1_DCM

    # Run the command from the image
    cmd += ["/usr/local/build/bin/physl", "--"]

    if perf:
        cmd += [
            "--dump-counters=py-csv.txt",
            "--dump-newick-tree=py-tree.txt",
            "--dump-dot=py-graph.txt",
            "--performance"]
    cmd += [
        "--print=result.py",
        "--hpx:ignore-batch-env",
        "--hpx:ini=hpx.parcel.tcp.enable=1",
        "--hpx:ini=hpx.parcel.mpi.enable=0",
        "--hpx:run-hpx-main",
        file_name
        ]
    
    job_id = None

    if perf or apex:

        py_src = f.get_python_src(f.backend.wrapped_function)

        physl_src_raw = f.get_physl_source()
        physl_src = prettify_physl(physl_src_raw)

        while True:
            randval = randint(1, 2<<31)
            perf_dir = "perf-%d" % randval
            job_id = "job-%d" % randval
            if not os.path.exists(perf_dir):
                break
        os.makedirs(perf_dir)
        with open("%s/py-src.txt" % perf_dir, "w") as fd:
            print(py_src, file=fd)
        with open("%s/physl-src.txt" % perf_dir, "w") as fd:
            print(physl_src, file=fd)

        apex_otf2_path = os.path.join(perf_dir, "./OTF2_archive")
        os.environ["APEX_OTF2_ARCHIVE_PATH"] = apex_otf2_path


    if apex:
        os.environ["APEX_OTF2"]="1"
    else:
        os.environ["APEX_OTF2"]="0"

    env = {}
    for k in os.environ:
        env[k] = os.environ[k]
    print(" ".join(cmd))
    p = s.Popen(cmd, env=env, stdout=s.PIPE, stderr=s.PIPE, universal_newlines=True)
    if perf or apex:
        with open("%s/label.txt" % perf_dir, "w") as fd:
            print(job_id, file=fd)
        def viz():
            nonlocal job_id, perf_dir
            visualizeDirInTraveler(job_id, perf_dir, True)
    else:
        def viz():
            print("No performance data to visualize")
    def download():
        print("No remote data to download (local job)")
    p.viz = viz
    p.download = download
    out, err = p.communicate()
    if err.strip != "":
        display(HTML("<div style='background: #FFDDDD; padding: 5pt; border: solid;'>"+html.escape(err)+"</div>"))
    if out.strip != "":
        print(out)
    return p
