backend_agave = {
  "baseurl" : "https://sandbox.agaveplatform.org",
  "tenant" : "sandbox",
  "user" : "{AGAVE_USER}",
  "utype" : "agave",
  "pass" : "{AGAVE_PASSWORD}",
}

backend_tapis = {
  "baseurl" : "https://api.tacc.utexas.edu",
  "tenant" : "tacc.prod",
  "user" : "{TAPIS_USER}",
  "utype" : "tapis",
  "pass" : "{TAPIS_PASSWORD}",
}

backends = {
    "tapis" : backend_tapis,
    "agave" : backend_agave
}

shelob = {
  "machine_user" : '{SHELOB_USER}',
  "machine" : 'shelob',
  "domain" : "hpc.lsu.edu",
  "queue" : "checkpt",
  "max_jobs_per_user" : 10,
  "max_jobs" : 20,
  "max_nodes" : 256,
  "max_run_time" : "72:00:00",
  "max_procs_per_node" : 16,
  "min_procs_per_node" : 16,
  "allocation" : "hpc_cmr",
  "scheduler" : "CUSTOM_TORQUE",
  "scratch_dir" : "/scratch/{machine_user}",
  "work_dir" : "/work/{machine_user}",
  "custom_directives" : """
    #PBS -A {allocation}
    #PBS -q {queue}
    #PBS -l nodes=${AGAVE_JOB_NODE_COUNT}:ppn=16
"""
}

rostam = {
  "machine_user" : '{ROSTAM_USER}',
  "machine" : 'rostam',
  "domain" : "cct.lsu.edu",
  "port" : 8000,
  "queue" : "rostam",
  "max_jobs_per_user" : 10,
  "max_jobs" : 20,
  "max_nodes" : 256,
  "max_run_time" : "1:00:00",
  "max_procs_per_node" : 16,
  "min_procs_per_node" : 1,
  "scheduler" : "SLURM",
  "scratch_dir" : "/home/{machine_user}",
  "work_dir" : "/home/{machine_user}"
}

systems = {
    "shelob" : shelob,
    "rostam" : rostam
}