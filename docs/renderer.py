"""Renderer for MyST with Bytewax formatting customizations."""

from __future__ import annotations

import re
import typing as t

import typing_extensions as te
from autodoc2.render.myst_ import MystRenderer
from autodoc2.utils import ItemData

_BASE_SPLIT = re.compile(r"(\s*[\[\]\(\),]+\s*)")


class BytewaxRenderer(MystRenderer):
    """Render the documentation as MyST.

    Based on the code in
    https://github.com/sphinx-extensions2/sphinx-autodoc2/blob/19587f0f56a17f6f9cc1c2c5614ad8c98d7293ff/src/autodoc2/render/myst_.py

    """

    @te.override
    def render_package(self, item: ItemData) -> t.Iterable[str]:
        if self.standalone and self.is_hidden(item):
            yield from ["---", "orphan: true", "---", ""]

        full_name = item["full_name"]

        yield f"# {{py:mod}}`{full_name}`"
        yield ""

        yield f"```{{py:module}} {full_name}"
        if self.no_index(item):
            yield ":noindex:"
        if self.is_module_deprecated(item):
            yield ":deprecated:"
        yield from ["```", ""]

        if self.show_docstring(item):
            yield f"```{{autodoc2-docstring}} {item['full_name']}"
            if parser_name := self.get_doc_parser(item["full_name"]):
                yield f":parser: {parser_name}"
            yield ":allowtitles:"
            yield "```"
            yield ""

        visible_submodules = [
            i["full_name"] for i in self.get_children(item, {"module", "package"})
        ]
        if visible_submodules:
            yield "## Submodules"
            yield ""
            yield "```{toctree}"
            yield ":titlesonly:"
            yield ""
            for name in sorted(visible_submodules):
                yield name
            yield "```"
            yield ""

        visible_children = [
            i["full_name"]
            for i in self.get_children(item)
            if i["type"] not in ("package", "module")
        ]
        if not visible_children:
            return

        for heading, types in [
            ("Data", {"data"}),
            ("Classes", {"class"}),
            ("Functions", {"function"}),
            ("External", {"external"}),
        ]:
            visible_items = list(self.get_children(item, types))
            if visible_items:
                yield from [f"## {heading}", ""]
                for i in visible_items:
                    yield from self.render_item(i["full_name"])
                    yield ""

    @te.override
    def render_class(self, item: ItemData) -> t.Iterable[str]:
        """Create the content for a class."""
        short_name = item["full_name"].split(".")[-1]
        constructor = self.get_item(f"{item['full_name']}.__init__")
        sig = short_name
        if constructor and "args" in constructor:
            args = self.format_args(
                constructor["args"], self.show_annotations(item), ignore_self="self"
            )
            sig += f"({args})"

        # note, here we can cannot yield by line,
        # because we need to look ahead to know the length of the backticks

        lines: t.List[str] = [f":canonical: {item['full_name']}"]
        if self.no_index(item):
            lines += [":noindex:"]
        lines += [""]

        # TODO overloads

        if item.get("bases") and self.show_class_inheritance(item):
            lines += (
                [
                    ":Bases:",
                ]
                + [
                    "    - " + self._reformat_cls_base_myst(b)
                    for b in item.get("bases", [])
                ]
                + [
                    "",
                ]
            )

        # TODO inheritance diagram

        if self.show_docstring(item):
            lines.append(f"```{{autodoc2-docstring}} {item['full_name']}")
            if parser_name := self.get_doc_parser(item["full_name"]):
                lines.append(f":parser: {parser_name}")
            lines.append("```")
            lines.append("")

            if self.config.class_docstring == "merge":
                init_item = self.get_item(f"{item['full_name']}.__init__")
                if init_item:
                    lines.extend(
                        [
                            "```{rubric} Initialization",
                            "```",
                            "",
                            f"```{{autodoc2-docstring}} {init_item['full_name']}",
                        ]
                    )
                    if parser_name := self.get_doc_parser(init_item["full_name"]):
                        lines.append(f":parser: {parser_name}")
                    lines.extend(["```", ""])

        for child in self.get_children(
            item, {"class", "property", "attribute", "method", "exception"}
        ):
            if (
                child["full_name"].endswith(".__init__")
                and self.config.class_docstring == "merge"
            ):
                continue
            for line in self.render_item(child["full_name"]):
                lines.append(line)

        backticks = self.enclosing_backticks("\n".join(lines))
        yield f"{backticks}{{py:{item['type']}}} {sig}"
        for line in lines:
            yield line
        yield backticks
        yield ""

    @te.override
    def render_data(self, item: ItemData) -> t.Iterable[str]:
        """Create the content for a data item."""
        short_name = item["full_name"].split(".")[-1]

        yield f"````{{py:{item['type']}}} {short_name}"
        yield f":canonical: {item['full_name']}"
        if self.no_index(item):
            yield ":noindex:"
        for prop in ("abstractmethod", "classmethod"):
            if prop in item.get("properties", []):
                yield f":{prop}:"

        anno = item.get("annotation")
        value = item.get("value")
        if value == "TypeVar(...)":
            yield f":type: {self.format_annotation('typing.TypeVar')}"
        elif value == "ParamSpec(...)":
            yield f":type: {self.format_annotation('typing.ParamSpec')}"
        else:
            if anno:
                yield f":type: {self.format_annotation(anno)}"

            # TODO: Could emit a `":value: >\n {value!r}"` here but
            # since the code isn't actually parsed, we don't get real
            # values. We'd want to use this for `TypeAlias` so we can
            # see the true types in the docs.

        yield ""
        if self.show_docstring(item):
            yield f"```{{autodoc2-docstring}} {item['full_name']}"
            if parser_name := self.get_doc_parser(item["full_name"]):
                yield f":parser: {parser_name}"
            yield "```"
            yield ""
        yield "````"
        yield ""

    @te.override
    def format_base(self, base: None | str) -> str:
        base = super().format_base(base)
        return f"~{base}"

    @te.override
    def _reformat_cls_base_myst(self, value: str) -> str:
        result = ""
        for sub_target in _BASE_SPLIT.split(value.strip()):
            # `re.split` will return the delimiters if there is a
            # capture, and `_BASE_SPLIT` does have one.
            if _BASE_SPLIT.match(sub_target):
                # So if it's in `[](), `, then quote that to make it
                # look like code.
                result += f"`{sub_target}`"
            elif sub_target:
                # Otherwise xref.
                result += f"{{py:obj}}`{self.format_base(sub_target)}`"

        return result
