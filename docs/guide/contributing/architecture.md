# Architecture History

## Timely Roots

Bytewax has its roots in the Rust project [Timely
Dataflow](https://github.com/TimelyDataflow/timely-dataflow) and
although it is not necessary to understand Timely Dataflow to use
Bytewax, it is a core component (the distributed processing engine) of
Bytewax and many of the concepts are passed through. The short history
of Timely Dataflow is that there was a group of people that included
Frank McSherry and Derek G. Murray working for Microsoft Research
about a decade ago who worked on a project called
[Naiad](https://www.youtube.com/watch?v=yyhMI9r0A9E). Naiad was a
system designed around the concept of how you could create a
distributed system to execute dataflow programs that would be
performant across batch, stream, and graph processing. From the [paper
abstract](https://dl.acm.org/doi/10.1145/2517349.2522738):

> Naiad is a distributed system for executing data parallel, cyclic
> dataflow programs. It offers the high throughput of batch
> processors, the low latency of stream processors, and the ability to
> perform iterative and incremental computations. Although existing
> systems offer some of these features, applications that require all
> three have relied on multiple platforms, at the expense of
> efficiency, maintainability, and simplicity. Naiad resolves the
> complexities of combining these features in one framework.
>
> A new computational model, timely dataflow, underlies Naiad and
> captures opportunities for parallelism across a wide class of
> algorithms. This model enriches dataflow computation with timestamps
> that represent logical points in the computation and provide the
> basis for an efficient, lightweight coordination mechanism.
>
> We show that many powerful high-level programming models can be
> built on Naiad's low-level primitives, enabling such diverse tasks
> as streaming data analysis, iterative machine learning, and
> interactive graph mining. Naiad outperforms specialized systems in
> their target application domains, and its unique features enable the
> development of new high-performance applications.

At some point, Microsoft Research changed direction concerning the
Silicon Valley Lab (I don't know the real story, but some of the prior
reporting on it is
[here](https://www.vox.com/2014/9/18/11631044/microsoft-shuts-down-silicon-valley-research-lab-amid-broader-layoffs))
and in some way or another, the team moved on from there to start
other things. Many of the original team kept the idea of dataflow
programs core to what they next worked on. Frank McSherry went on to
spend time working on a Rust version of the core tenants of Naiad in
[Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow)
and [Differential
Dataflow](https://github.com/TimelyDataflow/differential-dataflow) and
Derek went on to bring the core dataflow programming concepts to
[Tensorflow](https://github.com/tensorflow/tensorflow).

## Of Pythons and Crabs

Bytewax leverages another open-source project to bring the Python
Native capabilities on top of a Rust processing engine. This project
is called PyO3 and is used by some of our friends at
[PyDantic](https://github.com/pydantic/pydantic-core),
[Polars](https://github.com/pola-rs/polars),
[ReRun](https://github.com/rerun-io/rerun) and more. PyO3 is a great
project that allows you to run Rust functions from Python like a
foreign function interface, and it also lets you embed Python in Rust.
Many more libraries are starting to leverage this combination of Rust
and Python as it provides a very powerful combination of the ease of
use and broad applicability of Python with the performance and
security benefits of Rust. In Bytewax we both call Rust from Python,
but also embed Python in Rust. This can get a little tricky and is the
less commonly used pattern used in PyO3.

## Putting It All Together

![Bytewax architecture diagram.](/assets/arch-diagram.svg)

%https://excalidraw.com/#json=3W3-eMAeFONI_dAvl-yJk,lnjMWljnHtyh9rmCTlHIEg

This is a rough diagram of the Bytewax architecture. The developer
interacts with the Bytewax API by writing Python code. They describe
the dataflow program via stringing together I/O connectors and
operators and provide the Python logic code that will execute any
transformations. Bytewax's functionality is implemented using Timely
at the base with PyO3 providing a Python interface. Bytewax provides a
few levels of Python interface to facilitate customizing and extending
our components.

When a dataflow is run with the `bytewax.run` command, the Python
interpreter starts up some Timely workers which will all run the
dataflow. State and progress from the dataflow are persisted from
memory to SQLite for recovery purposes and this can also be backed up
in the cloud (S3, Azure Blob etc.) when using the Bytewax platform.

## Bytewax at Sea Level

A user interfaces with the Bytewax API to construct a dataflow program
in Python that loosely resembles the image below. A dataflow will at
the very least run on one Timely worker, have one input and output
connector, and most likely, but not strictly necessary, have one
operator with some transformation code.

![Diagram of a Timely Worker containing multiple operators and Bytewax
Python code within those.](/assets/timely-worker.svg)

%https://excalidraw.com/#json=qiQd1RU8tUoA72E5nj76r,uhPLyY6F7i5hIaqDRMi7yg

There is a lot of coordination going on behind the scenes of what
seems quite simple when you run `python -m bytewax.run dataflow:flow
-w 3`. When that command is issued, Bytewax will construct the
dataflow on three separate workers. These all communicate via Timely
communication mechanisms and are essentially independent aside from
stateful operations when data is exchanged to ensure the right data
lands on the right worker.

The connectors, progress tracking, recovery mechanisms, and scaling of
workers are all highly interconnected between Bytewax code, storage
infrastructure, and Timely mechanisms. To read more about this, please
refer to the <project:/user_guide/concepts/rescaling.md>,
<project:/user_guide/concepts/recovery.md> and
<project:/user_guide/advanced-concepts/custom-connectors.md> sections.
