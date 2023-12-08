# Profiling Bytewax

So you think Bytewax is slow? Let's do some profiling to produce CPU
flame graphs.

Instructions are Linux only as of now.

## 0. Installation

Install `perf` on Ubuntu with:

```bash
sudo apt install linux-tools-common
```

We'll also be using Hotspot to visualize the flame graphs.

```bash
sudo apt install hotspot
```

Python must be compiled with the appropriate options to be able to
reconstruct call stacks. This is easiest done via
[`pyenv`](https://github.com/pyenv/pyenv).

```bash
curl https://pyenv.run | bash
```

## 1. Compile Correctly

### Python

First let's compile Python with the appropriate flags.

```bash
PYTHON_CONFIGURE_OPTS="--enable-optimizations --with-lto" PYTHON_CFLAGS="-gdwarf -fno-omit-frame-pointer -mno-omit-leaf-frame-pointer" pyenv install 3.12.0
```

* `--enable-optimizations` and `--with-lto` give us a "release" build.
  This makes the compile quite long; you can leave this out but it
  will change the timings of the resulting dataflow slightly.

* `-gdwarf` adds in debugging symbols.

* `-fno-omit-frame-pointer` and `-mno-omit-leaf-frame-pointer` helps
  call stack recreation.

I encourage using Python 3.12+ because it includes features to see
Python functions in the call stack.

### Bytewax

The performance characteristics of the library changes drastically
when compiled in **release** mode, so before doing any profiling /
benchmarking, we need to ensure we compile in that mode.

```bash
RUSTFLAGS="-C debuginfo=1 -C force-frame-pointers" maturin develop --release
```

* `-C debuginfo=1` adds enough debug info to name functions.

* `-C force-frame-pointers` helps call stack recreation.

## 2. Profile Execution

We can now use [`perf`](https://www.brendangregg.com/perf.html) to
profile an execution of a dataflow:

```bash
perf record -F max --sample-cpu --call-graph=dwarf --aio -- python -X perf -m bytewax example_dataflow
```

This will write `./perf.data` for later analysis.

* `-F max` says to sample the call stack very fast.

* `--sample-cpu` says to record which CPU was running each thread
  during sampling.

* `--call-graph dwarf` says to use debug information to determine the
  call graph. This is crucial.

* `--aio` to improve performance while tracing.

* You can also use a `-p $PID` flag to profile an already running
  process.

* `-X perf` enables the [support for `perf`
  trampolines](https://docs.python.org/3/howto/perf_profiling.html) so
  you can see (some of the) Python call stack in the flamegraphs. This
  only works with Python 3.12+, so leave it out if you're profiling
  and older version. It appears that Python function calls performed
  directly from PyO3 are sometimes missing, so don't expect perfect
  vision, but this still helps.

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
