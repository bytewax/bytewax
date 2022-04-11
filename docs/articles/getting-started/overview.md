Bytewax is an open source Python framework for building highly scalable dataflows in a streaming or batch context.

Dataflow programming is a programming paradigm where program execution is conceptualized as data flowing through a series of operations or transformations.

Bytewax is a Python native binding to the [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow) library. Timely Dataflow is a dataflow processing library written in [Rust](https://www.rust-lang.org/).

At a high level, Bytewax provides a few major benefits:

- The operators in Bytewax are largely "data-parallel", meaning they can operate on independent parts of the data concurrently.
- The ability to express higher-level control constructs, like iteration.
- Bytewax allows you to develop and run your code locally, and then easily scale that code to multiple workers or processes without changes.