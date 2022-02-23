# Bytewax Changelog

## 0.8.0

- Capture operator no longer takes arguments. Items that flow through
  those points in the dataflow graph will be processed by the output
  handlers setup by each execution entry point. Every dataflow
  requires at least one capture.

- `Executor.build_and_run()` is replaced with four entry points for
  specific use cases:
  
  - `run_sync()` for synchronous exeuction in the current process. It
    returns all captured items to the calling process for you. Use
    this for prototyping in notebooks and basic tests.
  
  - `run_cluster()` for synchronous execution on a temporary
    machine-local cluster. It returns all captured items to the
    calling process for you. Use this for notebook analysis where you
    need parallelism.
    
  - `main_cluster()` for starting a machine-local cluster with more
    control over input and output. Use this for standalone scripts
    where you might need partitioned input and output.
  
  - `main_proc()` for starting a process that will participate in a
    cluster you are coordinating manually. Use this when starting a
    Kubernetes cluster.
  
- Adds `bytewax.parse` module to help with reading command line
  arguments and environment variables for the above entrypoints.
  
- Adds `manual_cluster` example.
