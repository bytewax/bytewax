# Writing Documentation

Documentation is Markdown files in the `/docs` folder. It is built
using [Sphinx](https://www.sphinx-doc.org/en/master/) and
[MyST](https://myst-parser.readthedocs.io/en/latest/index.html) for
Markdown parsing.

## Articles

Written articles live in `/docs/articles` in various sub-directories
by section.

If you add a new article, you should add the path to that file in the
appropriate `{toctree}` section listing in `/docs/index.md`. Articles
are not automatically added based on their location; although you'll
get a warning if Sphinx notices a Markdown file that isn't included
anywhere.

## Docstrings

API reference documentation is automatically during the Sphinx build
process via the [`sphinx-autodoc2`
extension](https://sphinx-autodoc2.readthedocs.io/en/latest/index.html)
generated from our Python source in `/pysrc`. The build process turns
the source into automatically generated Markdown files in
`/docs/apidocs`, which can then be referenced in any of the other
docs.

### Arguments

Docstrings should use MyST Markdown with arguments, return values,
etc. specified using [MyST field lists](inv:myst#syntax/fieldlists).
The field names should be the same as the
[Sphinx](inv:sphinx#info-field-lists).

```python
def my_func(x: int, y: str) -> str:
    """Do the cool thing.

    :arg x: Describe the X parameter.

    :arg y: Describe the Y parameter. If this is a really long line,
        you can wrap it with indentation. You can also use any
        **syntax** here you like.

    :returns: A description of the return value.

    """
    ...
```

If the function signature is coming from PyO3, you can use the
`:type:` and `:rtype:` field list to provide argument type hints.

```rust
/// Do the cool thing.
///
/// :arg x: Describe the X parameter.
///
/// :type x: int
///
/// :arg y: Describe the Y parameter. If this is a really long line,
///     you can wrap it with indentation. You can also use any
///     **syntax** here you like.
///
/// :type y: str
///
/// :returns: A description of the return value.
///
/// :rtype: str
#[pyfunction]
fn my_func(x: usize, y: String) -> String {
    todo!();
}
```

### Class and Module Variables

You can add "post-variable docstrings" to document these.

```python
from dataclasses import dataclass
from typing import TypeVar


X = TypeVar("X")
"""Type of a cool thing."""


@dataclass
class Container:
    x: int
    """This is the docstring for this attribute."""

    y: str
    """This is the docstring for this other attribute."""
```

## Cross References

See [MyST's documentation on cross
referencing](inv:myst#syntax/cross-referencing) for all the ways this
can work. I'll give a quick summary here.

### Other Markdown Files

To link to an entire article you can use normal Markdown link syntax
with a path to the Markdown file. The path can be relative or
absolute; if absolute it is rooted in `/docs`.

```markdown
Read the [article on recovery](/articles/concepts/recovery.md).
```

Appears as:

> Read the [article on recovery](/articles/concepts/recovery.md).

You can also automatically generate link text by using the Markdown
autolink syntax with the scheme `project:` and a path.

```markdown
Read about <project:/articles/concepts/recovery.md>
```

Appears as:

> Read about <project:/articles/concepts/recovery.md>

(xref-specific-section)=
### A Specific Section

The system does not automatically generate xref links for headings.
You can manually add a reference name to any heading via the
`(ref-name)=` syntax just before it. In general, just add refs for
sections you know you want to reference elsewhere.

```markdown
(xref-specific-section)=
### A Specific Section
```

You can then reference it via normal Markdown link syntax with the URI
being just `#ref-name`.

```markdown
Read [how to link to a specific section](#xref-specific-section)
```

Appears as:

> Read [how to link to a specific section](#xref-specific-section)

Or the autolink syntax with the scheme `project:` and then a
`#ref-name`.

```markdown
Read about linking to <project:#xref-specific-section>
```

Appears as:

> Read about linking to <project:#xref-specific-section>

:::{note}

Either the link URI has to either start with a `#` and be a global
Sphinx reference, or it is a path. You can't mix and match. This will
not work.

```markdown
Read [how to link to a specific section](/articles/contributing/writing-docs.md#xref-specific-section)
```

Instead make an explicit reference target with `(ref-name)=`.

:::

### API Docs

To link to a symbol in the Bytewax library, use the full dotted path
to it surrounded by `` ` `` and proceeded by `{py:obj}`.

```markdown
This operator returns a {py:obj}`bytewax.dataflow.Stream`.
```

Appears as:

> This operator returns a {py:obj}`bytewax.dataflow.Stream`.

You should always use the full dotted path to reference a name, but if
you don't want it to appear as a full dotted path because of the
context of the surrounding text, prefix the path with `~`.

```markdown
This operator returns a {py:obj}`~bytewax.dataflow.Stream`.
```

Appears as:

> This operator returns a {py:obj}`~bytewax.dataflow.Stream`.

## Example Code

````markdown
```python
flow = Dataflow()
```
````

## Shell Sessions

Use the language type `console` (instead of `bash`), and start
commands you run with `$` to get proper highlighting.

````markdown
```console
$ waxctl list
output here
```
````

Appears as:

> ```console
> $ waxctl list
> output here
> ```
