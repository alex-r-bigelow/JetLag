# jetlag
Custom interface to both Agave and Tapis for running and monitoring jobs.

The basic concept of universal is:
1. to use either Tapis or Agave
2. to describe a single machine that is:
   1. a storage machine
   2. an execution machine with `CLI`
   3. an execution machine with some `HPC` scheduler
3. Has a generic app which
   1. takes `input.tgz`
   2. unpacks and executes `run_dir/runapp.sh` from inside the `run_dir/` directory
   3. packs everything up into `output.tgz`
