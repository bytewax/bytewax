import subprocess
import sys
import textwrap


def _run(code: str) -> str:
    # Tracing is set up once per process, so run in a fresh one. A
    # deadlock shows up as a timeout instead of hanging the test run.
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_setup_tracing_twice_raises():
    out = _run(
        """
        from bytewax.tracing import setup_tracing

        tracer = setup_tracing()
        try:
            setup_tracing()
        except RuntimeError as ex:
            print(ex)
        else:
            print("second call did not raise")
        """
    )

    assert "tracing is already set up in this process" in out


def test_setup_tracing_raises_on_bad_config():
    out = _run(
        """
        from bytewax.tracing import OtlpTracingConfig, setup_tracing

        try:
            setup_tracing(OtlpTracingConfig(service_name="x", url="::bad::"))
        except RuntimeError as ex:
            print(ex)
        else:
            print("did not raise")
        """
    )

    assert "error building tracer" in out
