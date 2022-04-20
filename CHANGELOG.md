# Bytewax Changelog

## Latest

__Add any extra change notes here and we'll put them in the release
notes on GitHub when we make a new release.__

- Adds `bytewax.run_main()` as a way to test input and output builders
  without starting a cluster.
  
- Adds a `bytewax.testing` module with helpers for testing.

- `bytewax.run_cluster()` and `bytewax.spawn_cluster()` now take a
  `mp_ctx` argument to allow you to change the multiprocessing
  behavior. E.g. from "fork" to "spawn". Defaults now to "spawn".

## 0.8.0

- Capture operator no longer takes arguments. Items that flow through
  those points in the dataflow graph will be processed by the output
  handlers setup by each execution entry point. Every dataflow
  requires at least one capture.

- `Executor.build_and_run()` is replaced with four entry points for
  specific use cases:
  
  - `run()` for exeuction in the current process. It returns all
    captured items to the calling process for you. Use this for
    prototyping in notebooks and basic tests.
  
  - `run_cluster()` for execution on a temporary machine-local cluster
    that Bytewax coordinates for you. It returns all captured items to
    the calling process for you. Use this for notebook analysis where
    you need parallelism.
    
  - `spawn_cluster()` for starting a machine-local cluster with more
    control over input and output. Use this for standalone scripts
    where you might need partitioned input and output.
  
  - `cluster_main()` for starting a process that will participate in a
    cluster you are coordinating manually. Use this when starting a
    Kubernetes cluster.
  
- Adds `bytewax.parse` module to help with reading command line
  arguments and environment variables for the above entrypoints.
  
- Renames `bytewax.inp` to `bytewax.inputs`.
