# Profiling Bytewax

So you think Bytewax is slow? Let's do some profiling to produce CPU
flame graphs.

Instructions are Linux only as of now.

## 0. Installation

Install `perf` on Ubuntu with:

```bash
sudo apt install linux-tools-common
```

You will also need the debug symbols for the Python interpreter for
call stack reconstruction. Install the same version as Python you
have; this will install the latest, but there are specific versioned
ones, e.g. `python3.9-dbg`.

```bash
sudo apt install python3-dbg
```

We'll also be using Hotspot to visualize the flame graphs.

```bash
sudo apt install hotspot
```

## 1. Compile Correctly

The performance characteristics of the library changes drastically
when compiled in **release** mode, so before doing any profiling /
benchmarking, we need to ensure we compile in that mode.

We still need to retain relevant debugging info in the object file,
though. `-g` enables that and `-C force-frame-pointers=yes` helps with
the ability to reconstruct call stacks.

```bash
maturin develop --release -- -g -C force-frame-pointers=yes
```

## 2. Profile Execution

We can now use [`perf`](https://www.brendangregg.com/perf.html) to
profile an execution of a dataflow:

```bash
perf record -F 999 --sample-cpu --call-graph dwarf --aio -- python example_dataflow.py
```

This will write `./perf.data` for later analysis.

* `-F 999` says to sample the call stack at almost 1000 Hz.

* `--sample-cpu` says to record which CPU was running each thread
  during sampling.

* `--call-graph dwarf` says to use debug information to determine the
  call graph. This is crucial.

* `--aio` to improve performance while tracing.

* You can also use a `-p $PID` flag to profile an already running
  process.

As far as I can tell, it does not improve call stack reconstruction to
run the debug build of the interpreter when profiling (`python3d` vs
`python3`), we just need the debug symbols package installed. The
debug interpreter is slower, though.

## 3. Generate Flame Graphs

The best tool I've found for constructing flame graphs is
[Hotspot](https://github.com/KDAB/hotspot), a GUI visualizer.

Specifically, it seems to do two things better than using `perf report`:

1. It seems to be better at finding debug symbols where they actually
   get installed.

2. It uses a [custom `perf.data`
   parser](https://github.com/qt-creator/perfparser) that can use
   DWARF data to reconstruct call stacks when it is available, but
   [will fall back to using frame pointers when
   not](https://github.com/KDAB/hotspot#broken-backtraces). My
   understanding is that since some of the code being linked in does
   not have debug symbols, the frame pointers aid in call stack
   reconstruction.

Load the `perf.data` that was generate from your run.

It'll help to select the actual Timely worker threads for
analysis. It'll be the one that has the most solid orange bar in the
timeline at the bottom of the window since almost all the work is
occurring in it. Right click it and choose "Filter In On Thread".

## Further Work

Looks like one day we can use [Python's upcoming `perf`
support](https://docs.python.org/3.12/howto/perf_profiling.html) to be
able to see the Python call stack as well.

One thing that does improve the fidelity of the call stack
reconstruction more (giving you 10% fewer "mystery stacks") is to use
a Python interpreter you compile with `-fno-omit-frame-pointer`.

```bash
mkdir $HOME/dbgpython
git clone https://github.com/python/cpython.git
cd cpython
git checkout v3.10.7
CFLAGS='-fno-omit-frame-pointer' ./configure --enable-optimizations --with-lto --prefix $HOME/dbgpython
make
make test
make install
```

Then you can use this interpreter for a virtualenv:

```bash
$HOME/dbgpython/bin/python3.10 -m venv .venv
```

Then you can install Maturin and build Bytewax as above.
