import boto3

from bytewax.outputs import ManualOutputConfig


def output_builder(table):
    """Construct an output function to map Bytewax's (epoch, item) structure
    to boto3's required structure"""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table)

    def put_item(item):
        _epoch, item = item
        table.put_item(Item=item)

    return put_item


class DynamoDBOutputConfig(ManualOutputConfig):
    """Write output of a Dataflow to [DynamoDB](https://aws.amazon.com/dynamodb/).

    Creates a new item, or replaces an old item with a new item. If an item that has the same primary key as the new item already exists in the specified table, the new item completely replaces the existing item.

    Items are written to DynamoDB using [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html). For more information on authentication and configuration, please see the documentation for boto3.

    Items must be formatted as a dictionary, with value types that are supported by boto3. For more information about value types, see the documentation for [put_item](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item).

    Args:

        table: DynamoDB table to write output items to.

    Returns:

        Config object. Pass this as the `output_config` argument of the
        `bytewax.dataflow.Dataflow.output` operator.

    """

    def __new__(cls, table):
        """
        In classes defined by PyO3 we can only use __new__, not __init__
        """
        return super().__new__(cls, lambda wi, wn: output_builder(table))
