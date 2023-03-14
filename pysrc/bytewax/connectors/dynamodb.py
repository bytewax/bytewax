"""Connectors for AWS [DynamoDB](https://aws.amazon.com/dynamodb/).

Importing this module requires the
[`boto3`](https://github.com/boto/boto3) package to be installed.

"""
import boto3

from bytewax.outputs import DynamicOutput, StatelessSink


class _DynamoDBSink(StatelessSink):
    def __init__(self, table_name):
        self._dynamodb = boto3.resource("dynamodb")
        self._table = self._dynamodb.Table(table_name)

    def write(self, put_kwargs):
        self._table.put_item(**put_kwargs)

    def close(self):
        self._dynamodb.close()


class DynamoDBOutput(DynamicOutput):
    """Write output of a Dataflow to
    [DynamoDB](https://aws.amazon.com/dynamodb/).

    Incoming items must each be a kwarg dictionary for `boto3`'s
    [`Table.put_item`](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item).

    `boto3`'s [authentication environment
    variables](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration)
    must be setup for this output to work.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    Args:

        table_name: To write to.

    """

    def __init__(self, table_name: str):
        self._table_name = table_name

    def build(self, worker_index, worker_count):
        return _DynamoDBSink(self._table_name)
