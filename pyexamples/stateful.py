import bytewax


def count_to_10_with_dups():
    for i in range(10):
        for _ in range(2):
            yield (1, i)


# One way to structure custom stateful operations! Initialize state in
# instance variables and make instance callable. Instantiate a new
# class for each use.

# Remember, you still have the aggregate and state_machine operators
# that let you collect state. They have built in support for the
# common pattern of grouping by a key, and also automatically handle
# multiple workers correctly. But if you need finer grained control,
# this pattern is available.


class NumberedInspectorTime:
    """Keeps track of the number of items seen and prints out that count
    with each item and timestamp.

    Use with flow.inspect_time().

    """

    def __init__(self, label):
        self._label = label
        self._i = 0

    def __call__(self, t, x):
        self._i += 1
        print(f"{self._label} item {self._i}: {x} @ {t}")


class DuplicateRemover:
    """Returns whether the current object is equal to the last object
    seen.

    Use with flow.filter()."""

    def __init__(self):
        self._last = object()  # Will never be equal to anything else.

    # Allow flow.filter() to "call" this instance.
    def __call__(self, x):
        is_dup = x == self._last
        self._last = x
        return not is_dup  # Pass through new items.


class Summer:
    """Returns the sum so far of all items.

    Use with flow.map()."""

    def __init__(self):
        # Store state in instance.
        self._sum = 0

    # Allow flow.map() to "call" this instance.
    def __call__(self, x):
        self._sum += x
        return self._sum


ec = bytewax.Executor()
flow = ec.Dataflow(count_to_10_with_dups())
flow.inspect_epoch(NumberedInspectorTime("Input"))
flow.filter(DuplicateRemover())
flow.inspect_epoch(NumberedInspectorTime("After DuplicateRemover"))
flow.map(Summer())
flow.inspect_epoch(NumberedInspectorTime("After Summer"))


if __name__ == "__main__":
    ec.build_and_run()
