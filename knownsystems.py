backend_agave = {
  "baseurl" : "https://sandbox.agaveplatform.org",
  #"baseurl" : "https://tutorial.cct.lsu.edu",
  "tenant" : "sandbox",
  "user" : "{AGAVE_USER}",
  "utype" : "agave",
  "pass" : "{AGAVE_PASSWORD}",
}

backend_agave2 = {
  "baseurl" : "https://agave.cct.lsu.edu",
  "tenant" : "sandbox",
  "user" : "{AGAVE2_USER}",
  "utype" : "agave",
  "pass" : "{AGAVE2_PASSWORD}",
}

backend_tapis = {
  "baseurl" : "https://api.tacc.utexas.edu",
  "tenant" : "tacc.prod",
  "user" : "{TAPIS_USER}",
  "utype" : "tapis",
  "pass" : "{TAPIS_PASSWORD}",
}

backend_tapis2 = {
  "baseurl" : "https://api.tacc.utexas.edu",
  "tenant" : "tacc.prod",
  "user" : "{TAPIS2_USER}",
  "utype" : "tapis",
  "pass" : "{TAPIS2_PASSWORD}",
}

backends = {
    "tapis" : backend_tapis,
    "agave" : backend_agave,
    "agave2" : backend_agave2
}

shelob = {
  "jetlag_id" : "shelob-funwave",
  "machine_user" : "funwave",
  "machine" : 'shelob',
  "domain" : "hpc.lsu.edu",
  "queue" : "checkpt",
  "max_jobs_per_user" : 10,
  "max_jobs" : 20,
  "max_nodes" : 256,
  "max_run_time" : "72:00:00",
  "max_procs_per_node" : 16,
  "min_procs_per_node" : 16,
  "allocation" : "hpc_cmr2",
  "scheduler" : "CUSTOM_TORQUE",
  "scratch_dir" : "/scratch/{machine_user}",
  "work_dir" : "/work/{machine_user}",
  "custom_directives" : """
    #PBS -A {allocation}
    #PBS -q {queue}
    #PBS -l nodes=${AGAVE_JOB_NODE_COUNT}:ppn=16
"""
}

shelob2 = {}
for k in shelob:
    shelob2[k] = shelob[k]
shelob2["jetlag_id"] = "shelob-sbrandt"
shelob2["machine_user"] = "sbrandt"

rostam = {
  "jetlag_id" : "rostam-sbrandt",
  "machine_user" : "sbrandt",
  "machine" : 'rostam',
  "domain" : "cct.lsu.edu",
  "port" : 22,
  "queue" : "marvin",
  "max_jobs_per_user" : 10,
  "max_jobs" : 20,
  "max_nodes" : 256,
  "max_run_time" : "1:00:00",
  "max_procs_per_node" : 16,
  "min_procs_per_node" : 1,
  "scheduler" : "SLURM",
  "scratch_dir" : "/home/{machine_user}",
  "work_dir" : "/home/{machine_user}",
  "allocation" : "medusa"
}

systems = {
    "shelob" : shelob,
    "rostam" : rostam
}
