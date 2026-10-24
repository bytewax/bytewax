import socket
import subprocess
import sys
import textwrap
import threading


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


def test_setup_tracing_second_call_keeps_first_exporter():
    # The first tracer exports to this collector. The exporter only
    # connects when it sends, so count the connections it accepts.
    collector = socket.create_server(("127.0.0.1", 0))
    collector_port = collector.getsockname()[1]
    # A port with nothing listening on it.
    unreachable = socket.create_server(("127.0.0.1", 0))
    unreachable_port = unreachable.getsockname()[1]
    unreachable.close()
    connections = []

    def accept_and_close():
        while True:
            try:
                conn, _ = collector.accept()
            except OSError:
                return
            connections.append(conn)
            conn.close()

    threading.Thread(target=accept_and_close, daemon=True).start()
    try:
        out = _run(
            f"""
            import os
            import time

            import bytewax.operators as op
            from bytewax.dataflow import Dataflow
            from bytewax.testing import TestingSink, TestingSource, run_main
            from bytewax.tracing import OtlpTracingConfig, setup_tracing

            # Export soon after a span ends, so the test does not wait
            # for the default five seconds.
            os.environ["OTEL_BSP_SCHEDULE_DELAY"] = "100"
            # Keep the first object alive: its runtime drives the exporter.
            first = setup_tracing(
                OtlpTracingConfig(service_name="first", url="http://127.0.0.1:{collector_port}")
            )
            try:
                setup_tracing(
                    OtlpTracingConfig(service_name="second", url="http://127.0.0.1:{unreachable_port}")
                )
            except RuntimeError as ex:
                print(ex)
            else:
                print("second call did not raise")

            flow = Dataflow("traced")
            s = op.input("inp", flow, TestingSource([1, 2, 3]))
            op.output("out", s, TestingSink([]))
            run_main(flow)
            # Give the batch exporter time to send the spans.
            time.sleep(1)
            """
        )
    finally:
        collector.close()

    assert "tracing is already set up in this process" in out
    assert connections, "the first tracer no longer exports"


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
