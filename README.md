[![Actions Status](https://github.com/bytewax/bytewax/workflows/CI/badge.svg)](https://github.com/bytewax/bytewax/actions)
[![PyPI](https://img.shields.io/pypi/v/bytewax.svg?style=flat-square)](https://pypi.org/project/bytewax/)
[![Bytewax User Guide](https://img.shields.io/badge/user-guide-brightgreen?style=flat-square)](https://docs.bytewax.io/)


<img src="https://user-images.githubusercontent.com/6073079/157465283-c106c4e5-301a-4e7a-a26e-586229356fdd.svg" width="400" />

Bytewax is an open source Python framework for building highly scalable dataflows in a streaming or batch context.

## Get started

Check out our [getting started guide](https://docs.bytewax.io/getting-started/overview/).

## About

Bytewax let's you build python based dataflows to process your data for augmentation, advanced analysis, machine learning and more. It is based on [Timely Dataflow](https://timelydataflow.github.io/timely-dataflow/), which is a cyclic dataflow computational model. At a high-level, dataflow programming is a programming paradigm where program execution is conceptualized as data flowing through a series of operator based steps. Operators are the processing primitives of bytewax. Each of them gives you a “shape” of data transformation, and you give them functions to customize them to a specific task you need. The combination of each operator and their custom logic functions we call a dataflow step. You chain together steps in a dataflow to solve your high-level data processing problem.

At a high level, Bytewax provides a few major benefits:

* The operators in Bytewax are largely “data-parallel”, meaning they can operate on independent parts of the data concurrently.
* The ability to express higher-level control constructs, like iteration.
* Bytewax allows you to develop and run your code locally, and then easily scale that code to multiple workers or processes without changes.
* Bytewax can be used in both a streaming and batch context
* Ability to leverage the Python ecosystem directly

## Community

[Slack](https://join.slack.com/t/bytewaxcommunity/shared_invite/zt-vkos2f6r-_SeT9pF2~n9ArOaeI3ND2w) Is the main forum for communication and discussion.

[GitHub Issues](https://github.com/bytewax/bytewax/issues) is reserved only for actual issues. Please use the slack community for discussions.

[Code of conduct code](https://github.com/bytewax/bytewax/blob/main/CODE_OF_CONDUCT.md)

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

For an exhaustive list of examples, checkout the [/examples](/examples) folder

## License

Bytewax is licensed under the [Apache-2.0](https://opensource.org/licenses/APACHE-2.0) license.

## Contributing

Contributions are welcome! This community and project would not be what it is without the [contributors](https://github.com/bytewax/bytewax/graphs/contributors). All contributions, from bug reports to new features, are welcome and encouraged.

</br>
</br>

<p align="center"> With ❤️ Bytewax</p> 
<p align="center"><img src="https://user-images.githubusercontent.com/6073079/157482621-331ad886-df3c-4c92-8948-9e50accd38c9.png" width="50" /> </p>
