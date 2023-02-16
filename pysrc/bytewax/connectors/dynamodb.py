"""Connectors for AWS [DynamoDB](https://aws.amazon.com/dynamodb/).

Importing this module requires the
[`boto3`](https://github.com/boto/boto3) package to be installed.

"""
import boto3

from bytewax.outputs import DynamicOutput


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

        table: DynamoDB table name.

    """

    def __init__(self, table):
        self.table = table

    def build(self):
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(self.table)

        def write(item):
            table.put_item(**item)

        return write
