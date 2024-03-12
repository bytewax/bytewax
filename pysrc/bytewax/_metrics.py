"""Interfaces for custom metrics."""

from prometheus_client import REGISTRY
from prometheus_client.exposition import generate_latest


def generate_python_metrics() -> str:
    """Generate Prometheus compatible metrics from the Python registry."""
    return generate_latest(REGISTRY).decode("utf-8")
