# Bytewax Changelog

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
  
- Adds `manual_cluster` example.

- Renames `bytewax.inp` to `bytewax.inputs`.

## 0.9.0

- `run()` and `run_cluster()` are now generators, allowing you to
  lazily process infinite inputs. Computation will only happen as you
  are consuming the output, though; use `list()` to block like the old
  behavior.

- `spawn_cluster()` is async. Use `asyncio.run()` if you want to
  immediately block, like the old behavior.

- Adds a `run_main()` which mimics the interface of `run_cluster()`
  but run in the current thread. This will let you prototype IO
  builders.
