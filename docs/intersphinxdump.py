"""Dumps all the known configured intersphinx references to stdout.

You can then pipe this through `fzf` to find the one you want.

The output is in the [MyST xref
format](https://myst-parser.readthedocs.io/en/latest/syntax/cross-referencing.html#cross-project-intersphinx-links).
So you might need to munge the output if you want to use it with rST
role syntax.

"""

import warnings
from typing import Dict

from conf import intersphinx_mapping
from sphinx.ext import intersphinx


class _MockConfig:
    intersphinx_timeout = None
    tls_verify = False
    tls_cacerts = None
    user_agent = ""


class _MockApp:
    srcdir = ""
    config = _MockConfig()

    def warn(self, msg):
        warnings.warn(msg, stacklevel=2)


def _fetch_inventory(uri: str) -> Dict:
    return intersphinx.fetch_inventory(_MockApp(), "", uri)


def _main() -> None:
    for name, (target, conf_inv_uri) in intersphinx_mapping.items():
        if conf_inv_uri is None:
            inv_uri = target.removesuffix("/") + "/objects.inv"
        else:
            inv_uri = conf_inv_uri

        inv = _fetch_inventory(inv_uri)

        for domain, entries in inv.items():
            for entry, (_proj, _vers, _url_path, _display_name) in entries.items():
                fmt = f"inv:{name}:{domain}#{entry}"
                print(fmt)


if __name__ == "__main__":
    _main()
