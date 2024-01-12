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

class ClockConfig:
    """Base class for a clock config.

    This describes how a windowing operator should determine the
    current time and the time for each element.

    Use a specific subclass of this that matches the time definition
    you'd like to use.

    """

    ...

class RecoveryConfig:
    """Configuration settings for recovery.

    Args:
      db_dir (pathlib.Path): Local filesystem directory to search for
        recovery database partitions.

      backup_interval (typing.Optional[datetime.duration]): Amount of
        system time to wait to permanently delete a state snapshot after
        it is no longer needed. You should set this to the interval at
        which you are backing up the recovery partitions off of the
        workers into archival storage (e.g. S3). Defaults to zero
        duration.

      snapshot_serde (typing.Optional[bytewax.serde.Serde]):
        Serialization to use when encoding state snapshot objects in the
        recovery partitions. Defaults to
        `bytewax.serde.JsonPickleSerde`.

    """

    ...

    @property
    def backup_interval(self): ...
    @property
    def snapshot_serde(self): ...
    @property
    def db_dir(self): ...

class TracingConfig:
    """Base class for tracing/logging configuration.

    There defines what to do with traces and logs emitted by Bytewax.

    Use a specific subclass of this to configure where you want the
    traces to go.

    """

    ...

class WindowConfig:
    """Base class for a windower config.

    This describes the type of windows you would like.

    Use a specific subclass of this that matches the window definition
    you'd like to use.

    """

    ...

class WindowMetadata:
    """Contains information about a window."""

    ...

    @property
    def open_time(self):
        """The time that the window starts."""
        ...

    @property
    def close_time(self):
        """The time that the window closes.

        For some window types like `SessionWindow`, this value can
        change as new data is received.

        """
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

    >>> from bytewax.dataflow import Dataflow
    >>> from bytewax.testing import TestingInput
    >>> from bytewax.connectors.stdio import StdOutput
    >>> flow = Dataflow("my_df")
    >>> flow.input("inp", TestingInput(range(3)))
    >>> flow.capture(StdOutput())
    >>> # In a real example, use "host:port" of all other workers.
    >>> addresses = []
    >>> proc_id = 0
    >>> cluster_main(flow, addresses, proc_id)
    0
    1
    2

    Args:
      flow (bytewax.dataflow.Dataflow): Dataflow to run.

      addresses (typing.List[str]): List of host/port addresses for
        all processes in this cluster (including this one).

      proc_id (int): Index of this process in cluster; starts from 0.

      epoch_interval (typing.Optional[datetime.timedelta]): System
        time length of each epoch. Defaults to 10 seconds.

      recovery_config (typing.Optional[bytewax.recovery.RecoveryConfig]):
        State recovery config. If `None`, state will not be persisted.

      worker_count_per_proc (int): Number of worker threads to start
        on each process. Defaults to `1`.

    """
    ...

def init_db_dir(db_dir, count):
    """Create and init a set of empty recovery partitions.

    Args:
      db_dir (path.Path): Local directory to create partitions in.

      count (int): Number of partitions to create.

    """
    ...

def run_main(flow, *, epoch_interval=None, recovery_config=None):
    """Execute a dataflow in the current thread.

    Blocks until execution is complete.

    This is only used for unit testing. See `bytewax.run`.

    >>> from bytewax.dataflow import Dataflow
    >>> from bytewax.testing import TestingInput, run_main
    >>> from bytewax.connectors.stdio import StdOutput
    >>> flow = Dataflow("my_df")
    >>> flow.input("inp", TestingInput(range(3)))
    >>> flow.capture(StdOutput())
    >>> run_main(flow)
    0
    1
    2

    Args:
      flow (bytewax.dataflow.Dataflow): Dataflow to run.

      epoch_interval (typing.Optional[datetime.timedelta]): System
        time length of each epoch. Defaults to 10 seconds.

      recovery_config (typing.Optional[bytewax.recovery.RecoveryConfig]):
        State recovery config. If `None`, state will not be persisted.

    """
    ...

def setup_tracing(tracing_config=None, log_level=None):
    """Setup Bytewax's internal tracing and logging.

    By default it starts a tracer that logs all `ERROR`-level messages
    to stdout.

    Note: To make this work, you have to keep a reference of the
    returned object.

    ```python
    tracer = setup_tracing()
    ```

    Args:
      tracing_config (TracingConfig): The specific backend you want to
        use.

      log_level (str): String of the log level. One of `"ERROR"`,
        `"WARN"`, `"INFO"`, `"DEBUG"`, `"TRACE"`.

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

    Args:
      dt_getter (typing.Callable): Returns the timestamp for an item.
        The `datetime` returned must have tzinfo set to `timezone.utc`.
        E.g. `datetime(1970, 1, 1, tzinfo=timezone.utc)`

      wait_for_system_duration (datetime.timedelta): How much system
        time to wait before considering an event late.

    Returns:
      Config object. Pass this as the `clock_config` parameter to
      your windowing operator.

    """

    ...

    @property
    def wait_for_system_duration(self): ...
    @property
    def dt_getter(self): ...

class SystemClockConfig(ClockConfig):
    """Use the current system time as the timestamp for each item.

    The watermark is also the current system time.

    If the dataflow has no more input, all windows are closed.

    Returns:
      Config object. Pass this as the `clock_config` parameter to
      your windowing operator.

    """

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

      OTEL_EXPORTER_JAEGER_AGENT_HOST="127.0.0.1"
      OTEL_EXPORTER_JAEGER_AGENT_PORT="6831"

    Args:
      service_name (str): Identifies this dataflow in Jaeger.

      endpoint (typing.Optional[str]): Connection info. Takes
        precidence over env vars. Defaults to `"127.0.0.1:6831"`.

      sampling_ratio (float): Fraction of traces to send between `0.0`
        and `1.0`.

    """

    ...

    @property
    def sampling_ratio(self): ...
    @property
    def service_name(self): ...
    @property
    def endpoint(self): ...

class OtlpTracingConfig(TracingConfig):
    """Send traces to the OpenTelemetry collector.

    See [OpenTelemetry collector
    docs](https://opentelemetry.io/docs/collector/) for more info.

    Only supports GRPC protocol, so make sure to enable it on your
    OTEL configuration.

    This is the recommended approach since it allows the maximum
    flexibility in what to do with all the data bytewax can generate.

    Args:
      service_name (str): Identifies this dataflow in Otlp.

      url (typing.Optional[str]): Connection info. Defaults to
        `"grpc:://127.0.0.1:4317"`.

      sampling_ratio (float): Fraction of traces to send between `0.0`
        and `1.0`.

    """

    ...

    @property
    def url(self): ...
    @property
    def sampling_ratio(self): ...
    @property
    def service_name(self): ...

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

    Args:
      gap (datetime.timedelta):
        Gap of inactivity before considering a session closed. The gap
        should not be negative.

    Returns:
      Config object. Pass this as the `window_config` parameter to
      your windowing operator.

    """

    ...

    @property
    def gap(self): ...

class SlidingWindow(WindowConfig):
    """Sliding windows of fixed duration.

    If `offset == length`, windows cover all time but do not overlap.
    Each item will fall in exactly one window. The `TumblingWindow`
    config will do this for you.

    If `offset < length`, windows overlap. Each item will fall in
    multiple windows.

    If `offset > length`, there will be gaps between windows. Each
    item can fall in up to one window, but might fall into none.

    Window start times are inclusive, but end times are exclusive.

    Args:
      length (datetime.timedelta): Length of windows.

      offset (datetime.timedelta): Duration between start times of
        adjacent windows.

      align_to (datetime.datetime): Align windows so this instant
        starts a window. This must be a constant. You can use this to
        align all windows to hour boundaries, e.g.

    Returns:
      Config object. Pass this as the `window_config` parameter to
      your windowing operator.

    """

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

    Args:
      length (datetime.timedelta): Length of windows.

      align_to (datetime.datetime): Align windows so this instant
        starts a window. This must be a constant. You can use this to
        align all windows to hour boundaries, e.g.

    Returns:
      Config object. Pass this as the `window_config` parameter to
      your windowing operator.

    """

    ...

    @property
    def length(self): ...
    @property
    def align_to(self): ...
