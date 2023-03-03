"""Connectors for GCP [BigQuery](https://cloud.google.com/bigquery).

Importing this module requires the
[`google-cloud-bigquery`](https://github.com/googleapis/python-bigquery)
package to be installed.

"""
from google.cloud.bigquery import Client

from bytewax.outputs import DynamicOutput


class BigQueryOutput(DynamicOutput):
    """Write output of a Dataflow to
    [BigQuery](https://cloud.google.com/bigquery).

    Incoming items must each be a kwargs dictionary for
    `google-cloud-bigquery`'s
    [`Client.insert_json_rows`](https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.client.Client#google_cloud_bigquery_client_Client_insert_rows_json)
    without the `table` kwarg

    [Google Cloud
    authentication](https://googleapis.dev/python/google-api-core/latest/auth.html)
    must be setup for this output to work.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    Args:

        table_ref: Table reference in the format
            `"{project_id}.{dataset_id}.{table_id}"`.

        credentials: Explicit GCP credentials object to use for
            authentication. See [Google's
            documentation](https://googleapis.dev/python/google-api-core/latest/auth.html#explicit-credentials)
            for more info.

    """

    def __init__(self, table_ref, credentials=None):
        self.table_ref = table_ref
        self.credentials = credentials

    def build(self):
        client = Client(credentials=self.credentials)
        table = client.get_table(self.table_ref)

        def write(item):
            errors = client.insert_rows_json(table=table, **item)
            if errors:
                raise RuntimeError(f"Errors while inserting rows: {errors!r}")

        return write
