"""
This should crash because we try to setup tracing twice.
"""

from bytewax.tracing import setup_tracing

tracer = setup_tracing(log_level="TRACE")
# XXX: Error here
tracer2 = setup_tracing(log_level="TRACE")
