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

class ClockConfig:
    """Base class for a clock config.

    This describes how a windowing operator should determine the
    current time and the time for each element.

    Use a specific subclass of this that matches the time definition
    you'd like to use.

    """

    ...

    def __init__(self): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

class RecoveryConfig:
    """Configuration settings for recovery.

    :arg db_dir: Local filesystem directory to search for recovery
        database partitions.

    :type db_dir: pathlib.Path

    :arg backup_interval: Amount of system time to wait to permanently
        delete a state snapshot after it is no longer needed. You
        should set this to the interval at which you are backing up
        the recovery partitions off of the workers into archival
        storage (e.g. S3). Defaults to zero duration.

    :type backup_interval: typing.Optional[datetime.timedelta]

    :arg snapshot_serde: Format to use when encoding state snapshot
        objects in the recovery partitions. Defaults to
        {py:obj}`~bytewax.serde.JsonPickleSerde`.

    :type snapshot_serde: typing.Optional[bytewax.serde.Serde]

    """

    ...

    def __init__(self, db_dir, backup_interval=None, snapshot_serde=None): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def backup_interval(self): ...
    @property
    def db_dir(self): ...
    @property
    def snapshot_serde(self): ...

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

class WindowConfig:
    """Base class for a windower config.

    This describes the type of windows you would like.

    Use a specific subclass of this that matches the window definition
    you'd like to use.

    """

    ...

    def __init__(self): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

class WindowMetadata:
    """Contains information about a window."""

    ...

    def __eq__(self, value, /):
        """Return self==value."""
        ...

    def __ge__(self, value, /):
        """Return self>=value."""
        ...

    def __getnewargs__(self, /):
        """Required boilerplate for unpickling."""
        ...

    def __getstate__(self, /):
        """Return a representation of this class as a PyDict for pickling."""
        ...

    def __gt__(self, value, /):
        """Return self>value."""
        ...

    def __init__(self, open_time, close_time): ...
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

    def __setstate__(self, /, state):
        """Unpickle from a PyDict of arguments."""
        ...

    @property
    def close_time(self):
        """The time that the window closes.

        For some window types like `SessionWindow`, this value can
        change as new data is received.

        """
        ...

    @property
    def open_time(self):
        """The time that the window starts."""
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

    ```python
    >>> from bytewax.dataflow import Dataflow
    >>> import bytewax.operators as op
    >>> from bytewax.testing import TestingSource, cluster_main
    >>> from bytewax.connectors.stdio import StdOutSink
    >>> flow = Dataflow("my_df")
    >>> s = op.input("inp", flow, TestingSource(range(3)))
    >>> op.output("out", s, StdOutSink())
    >>> # In a real example, use "host:port" of all other workers.
    >>> addresses = []
    >>> proc_id = 0
    >>> cluster_main(flow, addresses, proc_id)
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

def init_db_dir(db_dir, count):
    """Create and init a set of empty recovery partitions.

    :arg db_dir: Local directory to create partitions in.

    :type db_dir: pathlib.Path

    :arg count: Number of partitions to create.

    :type count: int

    """
    ...

def run_main(flow, *, epoch_interval=None, recovery_config=None):
    """Execute a dataflow in the current thread.

    Blocks until execution is complete.

    This is only used for unit testing. See `bytewax.run`.

    ```python
    >>> from bytewax.dataflow import Dataflow
    >>> import bytewax.operators as op
    >>> from bytewax.testing import TestingSource, run_main
    >>> from bytewax.connectors.stdio import StdOutSink
    >>> flow = Dataflow("my_df")
    >>> s = op.input("inp", flow, TestingSource(range(3)))
    >>> op.output("out", s, StdOutSink())
    >>> run_main(flow)
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

    % skip: next

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

def test_cluster(
    flow,
    *,
    epoch_interval=None,
    recovery_config=None,
    processes=1,
    workers_per_process=1,
):
    """Execute a Dataflow by spawning multiple Python processes.

    Blocks until execution is complete.

    This function should only be used for testing purposes.

    """
    ...

class AbortExecution(RuntimeError):
    """Raise this from `next_batch` to abort for testing purposes."""

    ...

class EventClockConfig(ClockConfig):
    """Use a getter function to lookup the timestamp for each item.

    The watermark is the largest item timestamp seen thus far, minus
    the waiting duration, plus the system time duration that has
    elapsed since that item was seen. This effectively means items
    will be correctly processed as long as they are not out of order
    more than the waiting duration in system time.

    If the dataflow has no more input, all windows are closed.

    :arg dt_getter: Returns the timestamp for an item. The `datetime`
        returned must have tzinfo set to `timezone.utc`. E.g.
        `datetime(1970, 1, 1, tzinfo=timezone.utc)`

    :type dt_getter: typing.Callable[[typing.Any], datetime.datetime]

    :arg wait_for_system_duration: How much time to wait before
        considering an event late.

    :type wait_for_system_duration: datetime.timedelta

    :returns: Config object. Pass this as the `clock_config` parameter
        to your windowing operator.

    """

    ...

    def __init__(self, dt_getter, wait_for_system_duration): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def dt_getter(self): ...
    @property
    def wait_for_system_duration(self): ...

class SystemClockConfig(ClockConfig):
    """Use the current system time as the timestamp for each item.

    The watermark is also the current system time.

    If the dataflow has no more input, all windows are closed.

    Returns:
      Config object. Pass this as the `clock_config` parameter to
      your windowing operator.

    """

    ...

    def __init__(self): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
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

class SessionWindow(WindowConfig):
    """Session windowing with a fixed inactivity gap.

    Each time a new item is received, it is added to the latest window
    if the time since the latest event is < `gap`. Otherwise a new
    window is created that starts at current clock's time.

    :::{warning}

    Currently, session windows do not support out-of-order data. Out
    of order data will be placed in their own sessions rather than
    merging adjacent sessions.

    Ensure that your data source is always in order if using an
    {py:obj}`~bytewax.operators.window.EventClockConfig`. Even if it
    is in-order, you cannot use event time session windows with any
    windowing join operator.

    {py:obj}`~bytewax.operators.window.SystemClockConfig` is always in
    order, so should be fine to use with any operator.

    :::

    :arg gap: Gap of inactivity before considering a session closed.
        The gap should not be negative.

    :type gap: datetime.timedelta

    :returns: Config object. Pass this as the `window_config`
        parameter to your windowing operator.

    """

    ...

    def __init__(self, gap): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def gap(self): ...

class SlidingWindow(WindowConfig):
    """Sliding windows of fixed duration.

    If `offset == length`, windows cover all time but do not overlap.
    Each item will fall in exactly one window. This would be
    equivalent to a
    {py:obj}`~bytewax.operators.window.TumblingWindow`.

    If `offset < length`, windows overlap. Each item will fall in
    multiple windows.

    If `offset > length`, there will be gaps between windows. Each
    item can fall in up to one window, but might fall into none.

    Window start times are inclusive, but end times are exclusive.

    :arg length: Length of windows.

    :type length: datetime.timedelta

    :arg offset: Duration between start times of adjacent windows.

    :type offset: datetime.timedelta

    :arg align_to: Align windows so this instant starts a window. This
        must be a constant. You can use this to align all windows to
        hour boundaries, e.g.

    :type align_to: datetime.datetime

    :returns: Config object. Pass this as the `window_config`
        parameter to your windowing operator.

    """

    ...

    def __init__(self, length, offset, align_to): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def align_to(self): ...
    @property
    def length(self): ...
    @property
    def offset(self): ...

class TumblingWindow(WindowConfig):
    """Tumbling windows of fixed duration.

    Each item will fall in exactly one window.

    Window start times are inclusive, but end times are exclusive.

    :arg length: Length of windows.

    :type length: datetime.timedelta

    :arg align_to: Align windows so this instant starts a window. This
        must be a constant. You can use this to align all windows to
        hour boundaries, e.g.

    :type align_to: datetime.timedelta

    :returns: Config object. Pass this as the `window_config`
        parameter to your windowing operator.

    """

    ...

    def __init__(self, length, align_to): ...
    def __new__(cls, *args, **kwargs):
        """Create and return a new object.  See help(type) for accurate signature."""
        ...

    @property
    def align_to(self): ...
    @property
    def length(self): ...
