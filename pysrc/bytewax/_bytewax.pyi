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

    One execution at a time may use a recovery store. Each partition is
    locked for as long as the execution that opened it runs. A second
    execution fails with a `RuntimeError` after a few seconds, without
    writing any recovery data. Wait for the first execution to exit,
    then retry.

    :arg db_dir: Local filesystem directory to search for recovery
        database partitions. It must be attached to a single host, such
        as a Kubernetes ReadWriteOnce block volume. A network share
        mounted on several hosts is not supported: SQLite WAL does not
        work across hosts, and locks on such a share may only exclude
        processes on the same client.

    :type db_dir: pathlib.Path

    :arg backup_interval: Amount of system time to wait to permanently
        delete a state snapshot after it is no longer needed. You
        should set this to the interval at which you are backing up
        the recovery partitions off of the workers into archival
        storage (e.g. S3). Defaults to zero duration.

    :type backup_interval: typing.Optional[datetime.timedelta]

    :arg snapshot_interval: Amount of system time between state
        snapshots and resume points. Snapshots are only taken at the
        end of epochs that are a multiple of this many epochs, plus at
        EOF, so resuming replays input from the last snapshot. Rounded
        up to a whole number of epochs. Defaults to `None`, which
        snapshots every epoch.

    :type snapshot_interval: typing.Optional[datetime.timedelta]

    """

    ...

    def __init__(self, db_dir, backup_interval=None, snapshot_interval=None): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def backup_interval(self): ...
    @property
    def db_dir(self): ...
    @property
    def snapshot_interval(self): ...

class TracingConfig:
    """Base class for tracing/logging configuration.

    There defines what to do with traces and logs emitted by Bytewax.

    Use a specific subclass of this to configure where you want the
    traces to go.

    """

    ...

    def __init__(self): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

def cli_main(
    flow,
    *,
    workers_per_process=1,
    process_id=None,
    addresses=None,
    epoch_interval=None,
    recovery_config=None,
): ...
def cluster_main(
    flow,
    addresses,
    proc_id,
    *,
    epoch_interval=None,
    recovery_config=None,
    worker_count_per_proc=1,
):
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

def execution_state():
    """The recovery, completion and lock state of this thread's execution.

    Returns whether the execution that a partition belongs to has a
    recovery config, whether it completed, and whether this process
    holds one of its recovery partition locks.

    `KafkaEosSink` reads this in its partitions' `close`, which runs on
    the worker thread. A partition closed with `completed` true knows
    that the recovery writes of its execution are durable. A process
    without a lock cannot keep a successor out, so it must not publish
    at close. A partition closed after its run has ended sees the run's
    locks only while the run is still alive, and all are `False` once
    it has returned.

    """
    ...

def init_db_dir(db_dir, count):
    """Create and init a set of empty recovery partitions.

    :arg db_dir: Local directory to create partitions in.

    :type db_dir: pathlib.Path

    :arg count: Number of partitions to create.

    :type count: int

    """
    ...

def request_graceful_drain():
    """Ask the execution in progress to drain and exit cleanly.

    A drain needs a single-process execution with a recovery config.
    Without a recovery config it would discard open windows and folds,
    so this raises `RuntimeError` while such an execution runs in this
    process. A multi-process cluster is not drained at all, so this
    raises while one runs too. A request made while no execution runs is
    kept for the next execution in this process, which then refuses to
    start unless it can be drained. The last execution to end clears it.

    Inputs stop reading new data and treat the request as EOF, so each
    partition snapshots its exact position and the execution closes
    through the normal EOF path. A following execution on the same
    recovery store then resumes with no replayed items.

    Stateful operators pause rather than finish. Their logic is kept
    and snapshotted, so an open window or fold is not emitted early,
    and the next execution continues it. Only a true end of input runs
    their final output.

    To drain on SIGTERM, install a handler before the execution starts.
    SIGTERM keeps its default action otherwise.

    % Skip this doctest because it installs a signal handler.

    ```python
    import signal
    from bytewax.run import request_graceful_drain

    def _drain_on_sigterm(signum, frame):
        # A second SIGTERM ends the process the usual way.
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        request_graceful_drain()

    signal.signal(signal.SIGTERM, _drain_on_sigterm)
    ```

    A multi-process cluster is resumed by recovery after a crash or a
    kill, as before. In a container, the process must receive the
    signal: run Python in exec form, not under `sh -c`, or under an init
    such as `tini`.

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

    Tracing can only be set up once per process. Calling this again
    raises a `RuntimeError`.

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

class InconsistentPartitionsError(ValueError):
    """Raised when two recovery partitions are from very different times.

    Bytewax only keeps around state snapshots for the backup interval.
    This means that if you are resuming a dataflow with one recovery
    partition much newer than another, it's not possible to find a
    consistent set of snapshots between them.

    This is probably due to not restoring a consistent set of recovery
    partition backups onto all workers or the backup process has been
    continously failing on only some workers.

    """

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

    def __init__(self, service_name, endpoint=None, sampling_ratio=1.0): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def endpoint(self): ...
    @property
    def sampling_ratio(self): ...
    @property
    def service_name(self): ...

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

    def __init__(self, service_name, url=None, sampling_ratio=1.0): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def sampling_ratio(self): ...
    @property
    def service_name(self): ...
    @property
    def url(self): ...

class MissingPartitionsError(FileNotFoundError):
    """Raised when an incomplete set of recovery partitions is detected."""

    ...

class NoPartitionsError(FileNotFoundError):
    """Raised when no recovery partitions are found on any worker.

    This is probably due to the wrong recovery directory being specified.

    """

    ...
