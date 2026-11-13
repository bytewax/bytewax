import os
import subprocess
import sys


def test_addon_in_another_path_entry_is_importable(tmp_path):
    # Add-on packages install modules into `bytewax` (for example
    # `bytewax.duckdb`) without an `__init__.py` of their own. One that
    # lives in another `sys.path` entry than bytewax itself, as with an
    # editable install or `PYTHONPATH`, must still be importable. Run
    # in a fresh interpreter so the path is set before `import bytewax`.
    addon = tmp_path / "bytewax" / "fake_addon"
    addon.mkdir(parents=True)
    (addon / "__init__.py").write_text("VALUE = 42\n")
    paths = [str(tmp_path), os.environ.get("PYTHONPATH", "")]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in paths if p)}

    out = subprocess.run(
        [sys.executable, "-c", "import bytewax.fake_addon as m; print(m.VALUE)"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "42"
