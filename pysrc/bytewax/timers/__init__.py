"""Tools for managing multiple timers."""

from datetime import datetime
from operator import itemgetter
from typing import Generic, Iterable, List, Optional, Tuple, TypeVar

from bytewax._utils import partition

T = TypeVar("T")
"""Type of alarm token."""


class MultiNotifier(Generic[T]):
    """Allow multiple notifications in a stateful operator.

    This helps you when you're writing a
    {py:obj}`bytewax.operators.StatefulBatchLogic` or
    {py:obj}`bytewax.operators.StatefulLogic` to keep track of
    multiple pending notifications.

    Generally, it'll be used something like this.

    ```{testcode}
    import copy

    from dataclasses import dataclass
    from datetime import datetime, timedelta, timezone
    from typing import Iterable, List, Tuple, Optional

    from bytewax.operators import StatefulBatchLogic
    from bytewax.timers import MultiNotifier


    @dataclass(frozen=True)
    class MyState:
        notifier: MultiNotifier


    class MyOperatorLogic(StatefulBatchLogic[V, V, MyState]):
        def __init__(self, notifier: MultiNotifier[str]) -> None:
            self.notifier = notifier

        @override
        def on_batch(self, values: List[V]) -> Tuple[Iterable[V], bool]:
            now = datetime.now(tz=timezone.utc)
            notify_in_ten_sec = now + timedelta(seconds=10)
            self.notifier.notify_at(notify_in_ten_sec, "token")

            # Do whatever else this logic should do.
            ...

        @override
        def on_notify(self) -> Tuple[Iterable[W], bool]:
            now = datetime.now(tz=timezone.utc)
            for token in self.notifier.due(now):
                # Do some work for this notification.
                ...

            # Do whatever else this logic should do.
            ...

        @override
        def notify_at(self) -> Optional[datetime]:
            return self.notifier.next_at()

        @override
        def snapshot(self) -> MyState:
            return MyState(copy.deepcopy(self.notifier))
    ```

    You must round-trip the `MultiNotifier` through the snapshotting
    system. This requires you to add it to whatever state you are
    snapshotting, and ensure that it is built back into your
    {py:obj}`bytewax.operators.StatefulBatchLogic` subclass in the
    {py:obj}`bytewax.operators.stateful_batch`'s `builder` callback
    function.

    """

    def __init__(self) -> None:
        """Init."""
        self._alarms: List[Tuple[datetime, T]] = []
        # We can't structure this as a list of callbacks because we
        # would need to serialize them into the recovery state.
        # Instead use tokens.

    def notify_at(self, at: datetime, token: T) -> None:
        """Mark that you want a notification at a given time.

        :arg at: Time to notify at.

        :arg token: That will be returned to you in the return list of
            {py:obj}`due`.

        """
        self._alarms.append((at, token))
        self._alarms.sort(key=itemgetter(0))

    def next_at(self) -> Optional[datetime]:
        """The soonest notification time, if any.

        This should be called from
        {py:obj}`bytewax.operators.StatefulBatchLogic.notify_at` or
        other relevant hooks which specify when for an operator to
        next awake.

        :returns:

        """
        try:
            at, token = self._alarms[0]
            return at
        except IndexError:
            return None

    def due(self, now: datetime) -> Iterable[T]:
        """Given the current time, find all due notifications.

        This should be called from
        {py:obj}`bytewax.operators.StatefulBatchLogic.on_notify` so
        that you can determine the exact notifications that have been
        triggered.

        :arg now: The current time.

        :returns: A list of all `token` given to {py:obj}`notify_at`
            for which the notification time has passed.

        """
        due, still_alarms = partition(self._alarms, lambda at_token: at_token[0] <= now)
        self._alarms = still_alarms
        return [token for at, token in due]
