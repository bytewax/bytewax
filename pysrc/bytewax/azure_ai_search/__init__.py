"""Azure Search Sink Implementation.

This module provides a dynamic sink for writing data to an Azure Search
index using Bytewax's streaming data processing framework. The sink is
capable of creating and managing a connection to Azure Search and
inserting documents in batches based on a user-defined schema.

Classes:
    AzureSearchSink: A dynamic sink that connects to an Azure Search service,
                     manages the index, and writes data in batches.
    _AzureSearchPartition: A stateless partition responsible for writing batches of
                           data to the Azure Search index.

Usage:
    - The `AzureSearchSink` class is used to define a sink that can be
      connected to a Bytewax dataflow.
    - The `build` method of `AzureSearchSink` creates an `_AzureSearchPartition`
      that handles the actual data writing process.
    - The sink supports inserting documents based on a user-defined schema,
      ensuring that the data is formatted correctly for the target index.

Logging:
    The module uses Python's logging library to log important events such
    as index operations, API requests, and error messages.

**Sample usage**:

```python
from bytewax.azure_ai_search import AzureSearchSink

schema = {
    "id": {"type": "string", "default": None},
    "content": {"type": "string", "default": None},
    "meta": {"type": "string", "default": None},
    "vector": {"type": "collection", "default": []},
}

# Initialize the AzureSearchSink with your schema
azure_sink = AzureSearchSink(
    azure_search_service="your-service-name",
    index_name="your-index-name",
    search_api_version="2024-07-01",
    search_admin_key="your-api-key",
    schema=schema,  # Pass the custom schema
)
```

## Installation and import sample

To install you can run

```bash
pip install bytewax-azure-ai-search
```

Then import

```python
from bytewax.azure_ai_search import AzureSearchSink
```

You can then add it to your dataflow

```python
azure_sink = AzureSearchSink(
    azure_search_service=service_name,
    index_name="bytewax-index",
    search_api_version="2024-07-01",
    search_admin_key=api_key,
    schema={
        "id": {"type": "string", "default": None},
        "content": {"type": "string", "default": None},
        "meta": {"type": "string", "default": None},
        "vector": {"type": "collection", "item_type": "single", "default": []},
    },
)

flow = Dataflow("indexing-pipeline")
input_data = op.input("input", flow, FileSource("data/news_out.jsonl"))
deserialize_data = op.map("deserialize", input_data, safe_deserialize)
extract_html = op.map("extract_html", deserialize_data, process_event)
op.output("output", extract_html, azure_sink)
```

**Note**

This installation includes the following dependencies:

```ssh
azure-search-documents==11.5.1
azure-common==1.1.28
azure-core==1.30.2
openai==1.44.1
```

These are used to write the vectors on the appropriate services based
on an Azure schema provided. We will provide an example in this README
for working versions of schema definition under these versions.

## Setting up Azure AI services

**This asumes you have set up an Azure AI Search service on the Azure
  portal. For more instructions, visit [their
  documentation](https://learn.microsoft.com/en-us/azure/search/search-create-service-portal)**

**Optional** To generate embeddings, you can set up an [Azure OpenAI
service](https://learn.microsoft.com/en-us/azure/ai-services/openai/how-to/create-resource?pivots=web-portal)
and deploy an embedding model such as `text-ada-002-embedding`

Once you have set up the resources, ensure to idenfity and store the
following information from the Azure portal:

* You Azure AI Search admin key
* You Azure AI Search service name
* You Azure AI Search service endpoint url

If you deployed an embedding model through Azure AI OpenAI service:

* You Azure OpenAI endpoint url
* You Azure OpenAI API key
* Your Azure OpenAI service name
* You Azure OpenAI embedding deployment name
* Your Azure OpenAI embedding name (e.g. text-ada-002-embedding`)

## Sample usage

You can find a complete example under the [`examples/` folder](https://github.com/bytewax/bytewax-azure-ai-search/tree/main/examples).

To execute the examples, you can generate a `.env` file with the following keywords:

```bash
# OpenAI
AZURE_OPENAI_ENDPOINT= <your-azure-openai-endpoint>
AZURE_OPENAI_API_KEY= <your-azure-openai-key>
AZURE_OPENAI_SERVICE=<your-azure-openai-named-service>
# Azure Document Search
AZURE_SEARCH_ADMIN_KEY=<your-azure-ai-search-admin-key>
AZURE_SEARCH_SERVICE=<your-azure-ai-search-named-service>
AZURE_SEARCH_SERVICE_ENDPOINT=<your-azure-ai-search-endpoint-url>

# Optional - if you prefer to generate embeddings with embedding
# models deployed on Azure

AZURE_EMBEDDING_DEPLOYMENT_NAME=<your-azure-openai-given-deployment-name>
AZURE_EMBEDDING_MODEL_NAME=<your-azure-openai-model-name>

# Optional - if you prefer to generate the embeddings with OpenAI
OPENAI_API_KEY=<your-openai-key>
```

Set up the connection and schema by running

```bash
python connection.py
```

You can verify the creation of the index was successful by visiting the portal.

![](https://github.com/bytewax/bytewax-azure-ai-search/blob/main/docs/images/sample-index.png)

If you click on the created index and press "Search" you can verify it
was created - but empty at this point.

![](https://github.com/bytewax/bytewax-azure-ai-search/blob/main/docs/images/sample-empty-index.png)

Generate the embeddings and store in Azure AI Search through the
bytewax-azure-ai-search sink

```bash
python -m bytewax.run dataflow:flow
```

Verify the index was populated by pressing "Search" with an empty query.

![](https://github.com/bytewax/bytewax-azure-ai-search/blob/main/docs/images/sample-filled-index.png)

**Note**

In the dataflow we initialized the custom sink as follows:

```python
from bytewax.azure_ai_search import AzureSearchSink

azure_sink = AzureSearchSink(
    azure_search_service=service_name,
    index_name="bytewax-index",
    search_api_version="2024-07-01",
    search_admin_key=api_key,
    schema={
        "id": {"type": "string", "default": None},
        "content": {"type": "string", "default": None},
        "meta": {"type": "string", "default": None},
        "vector": {"type": "collection", "item_type": "single", "default": []},
    },
)
```

The schema and structure need to match how you configure the schema
through the Azure AI Search Python API. For more information, [visit
their page](https://pypi.org/project/azure-search-documents/)

In this example:

```python
from azure.search.documents.indexes.models import (
    SimpleField,
    SearchFieldDataType,
)

# Define schema
fields = [
    SimpleField(
        name="id",
        type=SearchFieldDataType.String,
        searchable=True,
        filterable=True,
        sortable=True,
        facetable=True,
        key=True,
    ),
    SearchableField(
        name="content",
        type=SearchFieldDataType.String,
        searchable=True,
        filterable=False,
        sortable=False,
        facetable=False,
        key=False,
    ),
    SearchableField(
        name="meta",
        type=SearchFieldDataType.String,
        searchable=True,
        filterable=False,
        sortable=False,
        facetable=False,
        key=False,
    ),
    SimpleField(
        name="vector",
        type=SearchFieldDataType.Collection(SearchFieldDataType.Double),
        searchable=False,
        filterable=False,
        sortable=False,
        facetable=False,
        vector_search_dimensions=DIMENSIONS,
        vector_search_profile_name="myHnswProfile",
    ),
]
```

Complete examples can be found
[here](https://github.com/bytewax/bytewax/tree/main/examples/azure_ai_search)

"""

import json
import logging
from typing import Any, Dict, List, Optional, TypeVar, Union

import requests
from bytewax.outputs import DynamicSink, StatelessSinkPartition
from typing_extensions import override

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Type variables for flexibility in typing
V = TypeVar("V")

# Type aliases for common types
Document = Dict[str, Any]
Schema = Dict[str, Dict[str, Union[str, Optional[Any]]]]
Batch = List[Document]


class _AzureSearchPartition(StatelessSinkPartition):
    """Stateless partition writing batches of data to an Azure Search index.

    This class manages the connection to the Azure Search service and handles the
    formatting and insertion of data into the specified index.
    """

    def __init__(
        self,
        azure_search_service: str,
        index_name: str,
        search_api_version: str,
        search_admin_key: str,
        schema: Schema,
    ) -> None:
        """Initialize the _AzureSearchPartition."""
        self.azure_search_service = azure_search_service
        self.index_name = index_name
        self.search_api_version = search_api_version
        self.search_admin_key = search_admin_key
        self.schema = schema

    @override
    def write_batch(self, batch: Batch) -> None:
        """Write a batch of data to the Azure Search index."""
        search_endpoint = (
            f"https://{self.azure_search_service}.search.windows.net/"
            f"indexes/{self.index_name}/docs/index?api-version={self.search_api_version}"
        )
        headers = {
            "Content-Type": "application/json",
            "api-key": self.search_admin_key,
        }

        body: Dict[str, List[Document]] = {"value": []}

        for document in batch:
            if not self.validate_document(document, self.schema):
                logger.error(f"Invalid document: {document}")
                continue
            doc_body = {"@search.action": "upload"}
            for field_name, field_details in self.schema.items():
                # Add fields to the document body based on the schema provided
                if field_name == "vector":
                    doc_body[field_name] = document.get(field_name, "")
                else:
                    doc_body[field_name] = document.get(
                        field_name, field_details.get("default")
                    )

            body["value"].append(doc_body)

        body_json = json.dumps(body)

        logger.debug(f"Uploading document to Azure Search: {body_json}")

        try:
            response = requests.post(search_endpoint, headers=headers, data=body_json)
            response.raise_for_status()
            logger.info(f"Document uploaded successfully to index '{self.index_name}'.")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Request error occurred: {req_err}")
            logger.error(f"Response content: {response.text}")

    def validate_document(self, document: Document, schema: Schema) -> bool:
        """Validate the document against the schema."""
        for field_name, field_details in schema.items():
            if field_name in document:
                value = document[field_name]
                if field_details["type"] == "collection" and not isinstance(
                    value, list
                ):
                    logger.error(f"'{field_name}' should be a list, got {type(value)}.")
                    return False
                if field_details["type"] == "string" and not isinstance(value, str):
                    logger.error(
                        f"'{field_name}' should be a string, got {type(value)}."
                    )
                    return False
        return True


class AzureSearchSink(DynamicSink):
    """A dynamic sink for writing data to an Azure Search index in a dataflow."""

    def __init__(
        self,
        azure_search_service: str,
        index_name: str,
        search_api_version: str,
        search_admin_key: str,
        schema: Schema,
    ) -> None:
        """Initialize the AzureSearchSink."""
        self.azure_search_service = azure_search_service
        self.index_name = index_name
        self.search_api_version = search_api_version
        self.search_admin_key = search_admin_key
        self.schema = schema

    @override
    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _AzureSearchPartition:
        """Build a sink partition for writing to Azure Search."""
        return _AzureSearchPartition(
            azure_search_service=self.azure_search_service,
            index_name=self.index_name,
            search_api_version=self.search_api_version,
            search_admin_key=self.search_admin_key,
            schema=self.schema,
        )
