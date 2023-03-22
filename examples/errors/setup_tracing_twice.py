"""
This should crash because we try to setup tracing twice.
"""

import working

from bytewax.tracing import setup_tracing

tracer = setup_tracing(log_level="TRACE")
# XXX: Error here
tracer2 = setup_tracing(log_level="TRACE")


if __name__ == "__main__":
    from bytewax.execution import run_main
    run_main(working.flow)
    # from bytewax.execution import spawn_cluster
    # spawn_cluster(flow)
