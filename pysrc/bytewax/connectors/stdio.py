"""Connectors to console IO.

"""
from bytewax.outputs import DynamicOutput


class StdOutput(DynamicOutput):
    """Write each output item to stdout on that worker.

    Items consumed from the dataflow must look like a string. Use a
    proceeding map step to do custom formatting.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    def build(self):
        return print
