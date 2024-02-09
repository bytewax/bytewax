# noqa: D100
# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import re
import sys
from typing import Optional

import docutils.nodes as dn
import sphinx.addnodes as sn
from sphinx.application import Sphinx
from sphinx.environment import BuildEnvironment
from sphinx.errors import NoUri
from sphinx.ext.intersphinx import missing_reference

# -- Path setup --------------------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-extensions
# We have some custom plugins defined in this directory.

sys.path.insert(0, os.path.abspath("."))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Bytewax"
copyright = "2024, Bytewax, Inc"  # noqa: A001
author = "Bytewax, Inc."

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "autodoc2",
    "myst_parser",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx_favicon",
    "sphinxcontrib.mermaid",
]

intersphinx_mapping = {
    "confluent_kafka": (
        "https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/",
        None,
    ),
    "fastavro": ("https://fastavro.readthedocs.io/en/latest/", None),
    "myst": ("https://myst-parser.readthedocs.io/en/latest/", None),
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
    "typing_extensions": ("https://typing-extensions.readthedocs.io/en/latest/", None),
}
templates_path = ["_templates"]

# The default if none is specified after the code fence. Defaults to
# `python` otherwise.
highlight_language = "text"

# Warn on missing xrefs.
nitpicky = True
# Intersphinx xrefs that for some reason aren't listed. Ignore them.
nitpick_ignore = [
    (
        "py:class",
        "fastavro.types.AvroMessage",
    ),
    ("py:obj", "confluent_kafka.OFFSET_BEGINNING"),
    ("py:obj", "confluent_kafka.OFFSET_END"),
]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_show_copyright = False
html_show_sourcelink = False
html_static_path = ["_static"]

favicons = [
    {
        "rel": "icon",
        "href": "img/favicon.ico",
    },
    {
        "rel": "apple-touch-icon",
        "sizes": "192x192",
        "href": "img/apple.png",
        "color": "#fab90f",
    },
]

# -- Options for PyData theme ---------------------------------------------
# https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/index.html
html_css_files = ["css/variables.css", "css/custom.css"]
html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "back_to_top_button": False,
    "footer_start": ["copyright"],
    "footer_end": [],
    "article_footer_items": ["slack-footer.html"],
    # https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/header-links.html#icon-links
    "external_links": [
        {"name": "Platform Docs", "url": "https://platform.bytewax.io"},
    ],
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/bytewax",
            "icon": "fa-brands fa-github",
        },
        {
            "name": "Slack",
            "url": "https://join.slack.com/t/bytewaxcommunity/shared_invite/zt-1lhq9bxbr-T3CXxR_9RIUGb4qcBK26Qw",
            "icon": "fa-brands fa-slack",
        },
    ],
    "logo": {
        "alt_text": "Bytewax",
        "image_light": "_static/img/logo.svg",
        "image_dark": "_static/img/logo_dark.svg",
        "link": "https://bytewax.io",
        "text": "Docs",
    },
    # On the per-page right hand side TOC, show more depth by default.
    "show_toc_level": 3,
    "show_prev_next": True,
}

# -- Options for MyST --------------------------------------------------------
# https://myst-parser.readthedocs.io/en/latest/configuration.html

myst_enable_extensions = [
    # Allow using `:::{directive}`. This is recommended for directives
    # which might contain Markdown within, like admonitions. This
    # means syntax highlighters won't hide all the syntax within.
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#code-fences-using-colons
    "colon_fence",
    # Allows using the `:arg x: Description` synatx in docstrings.
    # Otherwise there's no "Markdown native" way to document
    # arguments.
    # https://myst-parser.readthedocs.io/en/latest/syntax/optional.html#field-lists
    "fieldlist",
]
myst_fence_as_directive = [
    # MyST usually uses the syntax ```{mermaid} to have a directive.
    # This will allow you to write ```mermaid which will render in
    # GitHub.
    "mermaid",
]
myst_number_code_blocks = [
    "python",
]

# -- Options for autodoc2 -----------------------------------------------------
# https://sphinx-autodoc2.readthedocs.io/en/latest/config.html

autodoc2_docstring_parser_regexes = [(r".*", "myst")]
autodoc2_hidden_objects = [
    "dunder",
    "inherited",
    "private",
]
# This is silly, but autodoc2 requires an `__all__` if the module
# matches the regexp and doesn't fallback to public names. So we have
# to list the modules with `__all__` explicitly.
autodoc2_module_all_regexes = [
    re.escape("bytewax.inputs"),
    re.escape("bytewax.operators.window"),
    re.escape("bytewax.recovery"),
    re.escape("bytewax.run"),
    re.escape("bytewax.testing"),
    re.escape("bytewax.tracing"),
]
# The build process for API docs has an automatic "pre-build" step
# which parses the Python code and then writes out Markdown files with
# the directives for all the objects. Those Markdown files are in this
# directory. This should not be committed because it is generated on
# each build.
autodoc2_output_dir = "apidocs"
# Python package to parse to generate Markdown API docs for in the
# above directory.
autodoc2_packages = ["../pysrc/bytewax"]
# Controls the generation of those Markdown files. We have some
# specific formatting requirements and inhereit from the built-in
# renderer. This is why we need the path adjustment at the beginning
# of this file.
autodoc2_render_plugin = "renderer.BytewaxRenderer"

# These are not autodoc2 config options, but they affect the display
# of Python object directives.

# Should defitions of each `:py:function` show the fully qualified
# name. This isn't necessary because the page is per-module.
add_module_names = False
# Wrap signatures
maximum_signature_line_length = 80
# Type hints in signatures should not include fully qualified names.
python_use_unqualified_type_names = True
# In TOC, `UnaryLogic.snapshot` appears as just `snapshot`. This isn't
# necessary because it's nested under `UnaryLogic` anyway.
toc_object_entries_show_parents = "hide"

# -- Hooks -------------------------------------------------------------------


def _ignore_private(
    app: Sphinx, env: BuildEnvironment, node: sn.pending_xref, contnode: dn.TextElement
) -> Optional[dn.reference]:
    """Ignore missing xrefs to private objects.

    Some of these are automatically created by `:py:function`
    directives due to signatures.

    """
    if node["refdomain"] == "py":
        path = node["reftarget"].split(".")
        if any(name.startswith("_") for name in path):
            raise NoUri()

    return None


def _resolve_type_aliases(
    app: Sphinx, env: BuildEnvironment, node: sn.pending_xref, contnode: dn.TextElement
) -> Optional[dn.reference]:
    """Try to resolve `:py:class:` xrefs as `:py:obj:` xrefs instead.

    For some reason the xrefs generated as part of `:py:function:` and
    `:py:class:` signatures for type hints assume all types are
    classes. Type aliases are not classes, they are `:py:data:`, so we
    use the `:py:obj:` "any" reftype to look through both.

    There are a whole mess of Sphinx bugs related to type aliases.
    Unclear when or how they'll be fixed.

    """
    if node["refdomain"] == "py" and node["reftype"] == "class":
        return app.env.get_domain("py").resolve_xref(
            env, node["refdoc"], app.builder, "obj", node["reftarget"], node, contnode
        )

    return None


def _resolve_typing_extensions(
    app: Sphinx, env: BuildEnvironment, node: sn.pending_xref, contnode: dn.TextElement
) -> Optional[dn.reference]:
    """Fixes to handle `typing_extensions.*` xrefs.

    For some reason the inventory that `typing_extensions` exports
    doesn't include the package name. So things like
    `typing_extensions.TypeAlias` and `typing_extensions.Self` are
    actually under the names `TypeAlias` and `Self`. This is not true
    for the stdlib versions of these types.

    Try rewriting to the stdlib version first to simplify the docs,
    then if you can't find that symbol in the stdlib, try again in the
    extension module.

    Also need to do the `:py:class:` to `:py:obj:` rewrite. For
    reasons described in `_resolve_type_alias`.

    """
    if node["refdomain"] == "py" and node["reftarget"].startswith("typing_extensions."):
        name = node["reftarget"][len("typing_extensions.") :]
        if node["reftype"] == "class":
            node["reftype"] = "obj"

        node["reftarget"] = f"typing.{name}"
        std_lib_xref = missing_reference(app, env, node, contnode)
        if std_lib_xref:
            return std_lib_xref

        node["reftarget"] = f"typing_extensions:{name}"
        return missing_reference(app, env, node, contnode)

    return None


def setup(app: Sphinx):
    """Install our custom Sphinx build hooks."""
    app.connect("missing-reference", _ignore_private)
    app.connect("missing-reference", _resolve_type_aliases)
    app.connect("missing-reference", _resolve_typing_extensions)
