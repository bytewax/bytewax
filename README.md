# Bytewax

[![Actions Status](https://github.com/bytewax/bytewax/workflows/CI/badge.svg)](https://github.com/bytewax/bytewax/actions)
[![PyPI](https://img.shields.io/pypi/v/bytewax.svg?style=flat-square)](https://pypi.org/project/bytewax/)
[![Bytewax User Guide](https://img.shields.io/badge/user-guide-brightgreen?style=flat-square)](https://docs.bytewax.io/)

Bytewax is an open source Python framework for building highly scalable dataflows in a streaming or batch context.

Dataflow programming is a programming paradigm where program execution is conceptualized as data flowing through a series of operations or transformations.

At a high level, Bytewax provides a few major benefits:

* The operators in Bytewax are largely “data-parallel”, meaning they can operate on independent parts of the data concurrently.
* The ability to express higher-level control constructs, like iteration.
* Bytewax allows you to develop and run your code locally, and then easily scale that code to multiple workers or processes without changes.

Bytewax uses [PyO3](https://github.com/PyO3/pyo3/) to provide Python bindings to the [Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/) Rust library.

Visit our [documentation site](https://docs.bytewax.io/) for full documentation

## Usage

Install the [latest release](https://github.com/bytewax/bytewax/releases/latest) with pip:

```shell
pip install bytewax
```

## Example

Here is an example of a simple dataflow program using Bytewax:

``` python
# examples/simple.py
from bytewax import Dataflow, run


flow = Dataflow()
flow.map(lambda x: x * x)
flow.capture()


if __name__ == "__main__":
    for epoch, y in sorted(run(flow, enumerate(range(10)))):
        print(y)
```

Running the program:

``` bash
python ./examples/simple.py
0
1
4
9
16
25
36
49
64
81
```

For a more complete example, and documentation on the available operators, check out the [User Guide](https://docs.bytewax.io/).

## License

Bytewax is licensed under the [Apache-2.0](https://opensource.org/licenses/APACHE-2.0) license.
