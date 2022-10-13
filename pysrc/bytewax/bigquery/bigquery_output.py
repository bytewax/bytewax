from google.cloud import bigquery

from bytewax.outputs import ManualOutputConfig

def output_builder(table_id):
    client = bigquery.Client()
    table = client.get_table(table_id)

    def write_rows(rows):
        errors = client.insert_rows_json(table, rows)  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    return write_rows


class BigqueryOutputConfig(ManualOutputConfig):
    """Write output of a Dataflow to [Bigquery](https://cloud.google.com/bigquery).

    Attempts to write items as new rows to an existing Bigquery table, consistent with the schema specifications of that table.

    Rows are written to Bigquery using [google-cloud-bigquery](https://pypi.org/project/google-cloud-bigquery/). For more information on authentication and configuration, please its documentation.

    Items flowing into the capture operator should be formatted as an array of dictionaries. The dictionary keys align with your column names, and value types should be compatible with your Bigquery table schema.
    
    Args:

        table_id: Table reference in the format of your Bigquery "{project_id}.{dataset_id}.{table_id}"

    Returns:

        Config object. Pass this as the `output_config` argument of the
        `bytewax.dataflow.Dataflow.output` operator.

    """

    def __new__(cls, table_id):
        """
        In classes defined by PyO3 we can only use __new__, not __init__
        """
        return super().__new__(cls, lambda wi, wn: output_builder(table_id))