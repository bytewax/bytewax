Have you ever wanted to implement particular weird semantic with your operators? Or maybe support something tricky? Or you have a fever and the only prescription is more decorators? Well with this guide you can!

## Custom Operators

You can define new custom operators in terms of already existing
operators. To do this you define an **operator function** and
decorate it with `operator`.

```python
from bytewax.dataflow import Stream, operator
import bytewax.operators as op


@operator
def add_to(step_id: str, up: Stream[int], y: int) -> Stream[int]:
    return op.map("shim_map", lambda x: x + y)
```

Each input or output `Stream` turns into a `Port` in the resulting
data model.

In order to generate the operator data model, and proper nesting of
operators, you must follow a few rules when writing your function:

- There must be a `step_id: str` argument, even if not used.

- All arguments, the return value, and return dataclass fields that
  are `Stream`s or `MultiStream`s must have type annotations. We
  recommend annotating all the arguments, the return value, and all
  fields in a return dataclass.

- You must use a `dataclass` to return multiple values.

- Argument and return dataclass field names must not overlap with the
  names defined on the `Operator` base class.

- `Stream`s, `MultiStream`s, and `Dataflow`s _must not appear in
  nested objects_: they either can be arguments, the return type
  directly, or the top-level fields of a dataclass that is the return
  type; nowhere else.

### Docstrings

A good docstring for a custom operator has a few things:

- A one line summary of the operator.

- A doctest example using the operator.

- Any arguments that are streams describe the required shape of that
  upstream.

- The return streams describe the shape of the data that is being sent
  downstream.
