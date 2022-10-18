import os
import re
import pdoc

from bytewax.execution import run_main, cluster_main

context = pdoc.Context()

bytewax = pdoc.Module("bytewax", context=context)
pdoc.link_inheritance(context)

# Use our templates
pdoc.tpl_lookup.directories.insert(0, "templates")

# HACK: These two functions aren't picked up by our module structure
# as a part of bytewax.execution, so we inject them here directly.
execution = bytewax.doc["execution"]
execution.doc["run_main"] = pdoc.Function("run_main", execution, run_main)
execution.doc["cluster_main"] = pdoc.Function("cluster_main", execution, cluster_main)


def recursive_htmls(mod):
    yield mod, mod.html()
    for submod in mod.submodules():
        yield from recursive_htmls(submod)


for mod, html in recursive_htmls(bytewax):
    filepath = os.path.join(
        "./html", *re.sub(r"\.html$", ".html", mod.url()).split("/")
    )
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(html)
