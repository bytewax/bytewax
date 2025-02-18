"""Sink to log data to Rerun from Bytewax.

See the top level documentation for usage.
"""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Literal, Optional

import rerun as rr

from bytewax.outputs import DynamicSink, StatelessSinkPartition

RerunEntity = rr.AsComponents | Iterable[rr.ComponentBatchLike]
"""The type used by Rerun to identify the argument for `rr.log`'s `entity` parameter.

It's defined as `rr.AsComponents | Iterable[rr.ComponentBatchLike]`.
"""


@dataclass
class RerunMessage:
    """Helper class that contains everything needed to log to Rerun.

    See [rerun's docs](https://docs.rerun.io) for details.

    :param str | list[str] entity_path: The path (or list of paths) of the entity
        to be logged. The connector prepends `dataflow/worker{worker_index}/` to the
        entity path, so that you can distinguish between workers when looking at the
        data.
    :param RerunEntity entity: The entity, or entities, you want to log to rerun.
    :param None | str timeline: Timeline name to log the entity to. Optional.
        If you set this parameter, you also have to set `time`, as the message
        needs a time in the specified timeline to be logged.
    :param None | float time: The time in seconds in the timeline specified above.
    :param bool timeless: Set the item as `timeless` when logging. See Rerun docs.
    :param static bool: Set the item as `static` when logging. See Rerun docs.
    """

    entity_path: str | list[str]
    entity: RerunEntity

    # Optional timing info
    timeline: Optional[str] = None
    time: Optional[float] = None

    # Optional properties
    timeless: bool = False
    static: bool = False


class _RerunPartition(StatelessSinkPartition[RerunMessage]):
    def __init__(
        self,
        recording: rr.RecordingStream,
        worker_index: int,
    ) -> None:
        self.recording = recording
        self.worker_index = worker_index

    def write_batch(self, items: list[RerunMessage]) -> None:
        for item in items:
            if item.timeline is not None:
                assert (
                    item.time is not None
                ), "time info required in RerunMessage if timeline is set"
                # set_time_seconds sets the time for the specified timeline
                # only in the current thread, so we can safely use this in
                # a multi worker/thread/process scenario.
                rr.set_time_seconds(item.timeline, item.time, recording=self.recording)
            path = f"dataflow/worker{self.worker_index}/{item.entity_path}"
            rr.log(
                path,
                item.entity,
                recording=self.recording,
                timeless=item.timeless,
                static=item.static,
            )


class RerunSink(DynamicSink):
    """Sink to handle Rerun instance and log data to it.

    Handle the initialization of Rerun in Bytewax's context, and logs items to it.

    Instances of this sink also offer a decorator that can be used to log execution
    info of any function in your user code to Rerun.
    See {py:obj}`RerunSink.rerun_log`.

    :param str application_id: The `application_id` to use in Rerun.
        You can use the same string as the `flow_id`.
        You MUST use the same `application_id` for all the sinks in your dataflow.
    :param str recording_id: The `recording_id`. This needs to be specified here
        so that all the workers can log to the same recording. You must use the
        same `recording_id` for all the sinks in your dataflow.
    :param str operating_mode: Rerun's operating mode. Possible values are: `"spawn"`,
        `"connect"`, `"save"`, `"serve"`

        The default mode is `"spawn"`, where the first worker that runs
        spawns the viewer on the machine you run the dataflow on, and all the other
        workers connect to it. Use this during local development.

        `"connect"` is similar to spawn, but no worker spawns the viewer, which has
        to be ran outside the dataflow, and all the workers will connect to it.
        If you use `"connect"` mode, you have to specify the `address` parameter too.

        `"save"` mode saves the recording of each worker in a separate file.
        Files can be opened all together with the rerun viewer to replay the
        session. This is suitable for production use if your workers have access
        to a filesystem where the files can be written.
        If you use `"save"` mode, you also need to specify the `save_dir` parameter.

        `"serve"` mode runs a web version of the viewer an opens the browser at
        the page. Workers send data to the browser viewer through websocket.
        Only works with a single worker, as there's seems to be no way
        of using this mode without spawning the webserver, so each worker
        would have to use a different viewer.

    :param None | str address: The address of the viewer to connect to.
        For `"connect"` mode only.

    :param None | pathlib.Path save_dir: The directory where to save recording files.
        For `"save"` mode only.

    """

    def __init__(
        self,
        application_id: str,
        recording_id: str,
        operating_mode: Literal["spawn", "connect", "save", "serve"] = "spawn",
        address: Optional[str] = None,
        save_dir: Optional[Path] = None,
    ) -> None:
        """Initialize Rerun."""
        self.operating_mode = operating_mode
        self.recording = rr.new_recording(
            application_id=application_id,
            recording_id=recording_id,
            make_thread_default=True,
            spawn=operating_mode == "spawn",
        )
        self.save_dir = None
        self.worker_index: None | int = None

        if operating_mode == "connect":
            assert (
                address is not None
            ), "Address must be set in operating_mode is 'connect'"
            print(address)
            rr.connect(addr=address, recording=self.recording)
        elif operating_mode == "save":
            assert (
                save_dir is not None
            ), "Path for save directory required if operating_mode is 'save'"
            # We save the dir locally and only call rr.save in the
            # `build` function so that each worker has its own file.
            assert save_dir.is_dir(), "save_dir must be a directory"
            self.save_dir = save_dir

            # Not sure why, but we also need to call `init` here even if
            # we already called `new_recording`, or this won't work.
            rr.init(application_id=application_id, recording_id=recording_id)
        elif operating_mode == "serve":
            # TODO: I think this can't work on multiple workers.
            rr.serve(recording=self.recording)
        elif operating_mode == "spawn":
            # Nothing to do
            pass
        else:
            msg = f"Invalid operating_mode: {operating_mode}"
            raise ValueError(msg)

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _RerunPartition:
        """Builds a sink partition for the Rerun sink."""
        self.worker_index = worker_index
        if self.save_dir is not None:
            file_path = self.save_dir / f"recording-{worker_index}.rrd"
            if file_path.exists():
                msg = (
                    f"File {file_path} already exists, remove it "
                    "or choose another directory to save the file"
                )
                raise FileExistsError(msg)
            rr.save(path=file_path, recording=self.recording)
        return _RerunPartition(self.recording, worker_index)

    def rerun_log(self, log_args: bool = False, log_return: bool = False) -> Callable:
        """Decorator to log any function's execution info to Rerun.

        Instantiate the sink outside the `output` operator, and use
        `@sink.rerun_log()` to decorate any function you want to
        see logged in rerun.

        This decorator will create a separate timeline, named `bytewax`,
        where for each function each worker will log the moment the function
        was started and for how long it ran.

        :param bool log_args: If this is `True`, the string representation
            of the arguments passed to the function will be logged.

        :param bool log_return: If this is `True`, the string representation
            of the returned value will be logged.

        Note: This needs to be a method of the RerunSink because we need
        to use the same `application_id` and `recording_id`, and we also
        need info on which worker this function is running at, so that
        we can log the correct info into Rerun.

        Example usage:
        ```python
        # ...
        sink = RerunSink("app_id", "rec_id")


        @sink.rerun_log(log_args=True, log_return=True)
        def my_function(item: int) -> int:
            return item * 2


        # ...
        ```
        """

        def inner(func: Callable) -> Callable:
            """Wrapper function for {py:obj}`RerunSink.rerun_log`."""
            import functools

            first = time.time()

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Callable:
                if self.worker_index is None:
                    msg = "worker_index is not set, did you forget to use the sink?"
                    raise RuntimeError(msg)
                since_start = time.time() - first
                start = time.time()
                res = func(*args, **kwargs)
                time_spent = time.time() - start
                rr.set_time_seconds("bytewax", since_start, recording=self.recording)
                rr.log(
                    f"bytewax/worker{self.worker_index}/{func.__name__}",
                    rr.Scalar(time_spent),
                    recording=self.recording,
                )
                if log_args:
                    rr.log(
                        f"bytewax/worker{self.worker_index}/{func.__name__}/args",
                        rr.TextLog(f"{args}", level=rr.TextLogLevel.TRACE),
                        recording=self.recording,
                    )
                    rr.log(
                        f"bytewax/worker{self.worker_index}/{func.__name__}/kwargs",
                        rr.TextLog(f"{kwargs}", level=rr.TextLogLevel.TRACE),
                        recording=self.recording,
                    )

                if log_return:
                    rr.log(
                        f"bytewax/worker{self.worker_index}/{func.__name__}/return",
                        rr.TextLog(f"{res}", level=rr.TextLogLevel.TRACE),
                        recording=self.recording,
                    )
                return res

            return wrapper

        return inner
