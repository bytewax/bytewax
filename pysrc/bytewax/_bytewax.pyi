# Programmatically generated stubs for `bytewax._bytewax`.

"""Internal Bytewax symbols from Rust.

These are re-imported elsewhere in the public `bytewax` module for
use.

"""

class BytewaxTracer:
    """Utility class used to handle tracing.

    It keeps a tokio runtime that is alive as long as the struct itself.

    This should only be built via `setup_tracing`.

    """
    ...

    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

class RecoveryConfig:
    """Configuration settings for recovery.

    :arg db_dir: Local filesystem directory to use for recovery
        database partitions.

    :type db_dir: pathlib.Path

    :arg backup: Class to use to save recovery files to a durable
        storage like amazon's S3.

    :type backup: typing.Optional[bytewax.backup.Backup]

    :arg batch_backup: Whether to take state snapshots at the end
        of the epoch, rather than at every state change. Defaults
        to False.

    :type batch_backup: bool

    """
    ...

    def __init__(self, db_dir, backup=None, snapshot_mode=None):
        ...

    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def backup(self):
        ...

    @property
    def db_dir(self):
        ...

    @property
    def snapshot_mode(self):
        ...

class SnapshotMode:
    ...

    Batch: object

    Immediate: object

    def __eq__(self, value, /):
        """Return self==value."""
        ...

    def __ge__(self, value, /):
        """Return self>=value."""
        ...

    def __gt__(self, value, /):
        """Return self>value."""
        ...

    def __int__(self, /):
        """int(self)"""
        ...

    def __le__(self, value, /):
        """Return self<=value."""
        ...

    def __lt__(self, value, /):
        """Return self<value."""
        ...

    def __ne__(self, value, /):
        """Return self!=value."""
        ...

    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    def __repr__(self, /):
        """Return repr(self)."""
        ...

class TracingConfig:
    """Base class for tracing/logging configuration.

    There defines what to do with traces and logs emitted by Bytewax.

    Use a specific subclass of this to configure where you want the
    traces to go.

    """
    ...

    def __init__(self):
        ...

    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

def cli_main(flow, *, workers_per_process=1, process_id=None, addresses=None, epoch_interval=None, recovery_config=None):
    ...

def cluster_main(flow, addresses, proc_id, *, epoch_interval=None, recovery_config=None, worker_count_per_proc=1):
    """Execute a dataflow in the current process as part of a cluster.

    This is only used for unit testing. See `bytewax.run`.

    Blocks until execution is complete.

    ```{testcode}
    from bytewax.dataflow import Dataflow
    import bytewax.operators as op
    from bytewax.testing import TestingSource, cluster_main
    from bytewax.connectors.stdio import StdOutSink

    flow = Dataflow("my_df")
    s = op.input("inp", flow, TestingSource(range(3)))
    op.output("out", s, StdOutSink())

    # In a real example, use "host:port" of all other workers.
    addresses = []
    proc_id = 0
    cluster_main(flow, addresses, proc_id)
    ```

    ```{testoutput}
    0
    1
    2
    ```

    :arg flow: Dataflow to run.

    :type flow: bytewax.dataflow.Dataflow

    :arg addresses: List of host/port addresses for all processes in
        this cluster (including this one).

    :type addresses: typing.List[str]

    :arg proc_id: Index of this process in cluster; starts from 0.

    :type proc_id: int

    :arg epoch_interval: System time length of each epoch. Defaults to
        10 seconds.

    :type epoch_interval: typing.Optional[datetime.timedelta]

    :arg recovery_config: State recovery config. If `None`, state will
        not be persisted.

    :type recovery_config:
        typing.Optional[bytewax.recovery.RecoveryConfig]

    :arg worker_count_per_proc: Number of worker threads to start on
        each process. Defaults to `1`.

    :type worker_count_per_proc: int

    """
    ...

def run_main(flow, *, epoch_interval=None, recovery_config=None):
    """Execute a dataflow in the current thread.

    Blocks until execution is complete.

    This is only used for unit testing. See `bytewax.run`.

    ```{testcode}
    from bytewax.dataflow import Dataflow
    import bytewax.operators as op
    from bytewax.testing import TestingSource, run_main
    from bytewax.connectors.stdio import StdOutSink
    flow = Dataflow("my_df")
    s = op.input("inp", flow, TestingSource(range(3)))
    op.output("out", s, StdOutSink())

    run_main(flow)
    ```

    ```{testoutput}
    0
    1
    2
    ```

    :arg flow: Dataflow to run.

    :type flow: bytewax.dataflow.Dataflow

    :arg epoch_interval: System time length of each epoch. Defaults to
        10 seconds.

    :type epoch_interval: typing.Optional[datetime.timedelta]

    :arg recovery_config: State recovery config. If `None`, state will
        not be persisted.

    :type recovery_config:
        typing.Optional[bytewax.recovery.RecoveryConfig]

    """
    ...

def setup_tracing(tracing_config=None, log_level=None):
    """Setup Bytewax's internal tracing and logging.

    By default it starts a tracer that logs all `ERROR`-level messages
    to stdout.

    Note: To make this work, you have to keep a reference of the
    returned object.

    % Skip this doctest because it requires starting the webserver.

    ```python
    from bytewax.tracing import setup_tracing
    tracer = setup_tracing()
    ```

    :arg tracing_config: The specific backend you want to use.

    :type tracing_config: bytewax.tracing.TracingConfig

    :arg log_level: String of the log level. One of `"ERROR"`,
        `"WARN"`, `"INFO"`, `"DEBUG"`, `"TRACE"`. Defaults to
        `"ERROR"`.

    :type log_level: str

    """
    ...

class AbortExecution(RuntimeError):
    """Raise this from `next_batch` to abort for testing purposes."""
    ...

class JaegerConfig(TracingConfig):
    """Configure tracing to send traces to a Jaeger instance.

    The endpoint can be configured with the parameter passed to this
    config, or with two environment variables:

    ```sh
    OTEL_EXPORTER_JAEGER_AGENT_HOST="127.0.0.1"
    OTEL_EXPORTER_JAEGER_AGENT_PORT="6831"
    ```

    :arg service_name: Identifies this dataflow in Jaeger.

    :type service_name: str

    :arg endpoint: Connection info. Takes precidence over env vars.
        Defaults to `"127.0.0.1:6831"`.

    :type endpoint: str

    :arg sampling_ratio: Fraction of traces to send between `0.0` and
        `1.0`.

    :type sampling_ratio: float

    """
    ...

    def __init__(self, service_name, endpoint=None, sampling_ratio=1.0):
        ...

    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def endpoint(self):
        ...

    @property
    def sampling_ratio(self):
        ...

    @property
    def service_name(self):
        ...

class OtlpTracingConfig(TracingConfig):
    """Send traces to the OpenTelemetry collector.

    See [OpenTelemetry collector
    docs](https://opentelemetry.io/docs/collector/) for more info.

    Only supports GRPC protocol, so make sure to enable it on your
    OTEL configuration.

    This is the recommended approach since it allows the maximum
    flexibility in what to do with all the data bytewax can generate.

    :arg service_name: Identifies this dataflow in OTLP.

    :type service_name: str

    :arg url: Connection info. Defaults to `"grpc:://127.0.0.1:4317"`.

    :type url: str

    :arg sampling_ratio: Fraction of traces to send between `0.0` and
        `1.0`.

    :type sampling_ratio: float

    """
    ...

    def __init__(self, service_name, url=None, sampling_ratio=1.0):
        ...

    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def sampling_ratio(self):
        ...

    @property
    def service_name(self):
        ...

    @property
    def url(self):
        ...
