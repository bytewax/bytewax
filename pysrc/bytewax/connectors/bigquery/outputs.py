from google.cloud import bigquery

from bytewax.outputs import ManualOutputConfig


class BigQueryOutputConfig(ManualOutputConfig):
    """Write output of a Dataflow to [Bigquery](https://cloud.google.com/bigquery).

    Attempts to write items as new rows to an existing Bigquery table, consistent with the schema specifications of that table.

    Rows are written to Bigquery using [google-cloud-bigquery](https://pypi.org/project/google-cloud-bigquery/).
    For more information on authentication and configuration, please see its documentation.

    Items flowing into the capture operator should be formatted as dictionaries and will be passed as keyword
    arguments to the [`insert_json_rows` function of google-cloud-bigquery]
    (https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_insert_rows_json).
    The keyword "json_rows" should map to a dictionary representing the row to be inserted. The dictionary keys
    align with your column names, and value types should be compatible with your BigQuery table schema.

    Args:

        table_ref: Table reference in the format of your Bigquery "{project_id}.{dataset_id}.{table_id}"

    Returns:

        Config object. Pass this as the `output_config` argument of the
        `bytewax.dataflow.Dataflow.output` operator.

    """  # noqa

    def __new__(cls, table_ref):
        """
        In classes defined by PyO3 we can only use __new__, not __init__
        """

        def output_builder(wi, wc):
            client = bigquery.Client()
            table = client.get_table(table_ref)

            def output_handler(insert_kwargs):
                errors = client.insert_rows_json(table=table, **insert_kwargs)
                if errors != []:
                    raise Exception("Errors while inserting rows: {}".format(errors))

            return output_handler

        return super().__new__(cls, output_builder)
