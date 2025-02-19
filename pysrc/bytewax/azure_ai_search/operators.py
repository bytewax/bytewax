"""Operators for embedding generation using Azure OpenAI.

How to Use This Setup

1. Using Environment Variables:

```python
from bytewax.connectors.azure_openai import AzureOpenAIConfig, operators as aoop
from bytewax.dataflow import Dataflow

config = AzureOpenAIConfig()  # Automatically picks up env variables

flow = Dataflow("embedding-out")
input = aoop.input("input", flow, ...)
embedded = aoop.generate_embeddings("embedding_op", input, config)
aoop.output("output", embedded, ...)
```

Passing Credentials Directly:

```python
from bytewax.connectors.azure_openai import AzureOpenAIConfig, operators as aoop
from bytewax.dataflow import Dataflow

config = AzureOpenAIConfig(
    api_key="your-api-key",
    service_name="your-service-name",
    deployment_name="your-deployment-name",
)

flow = Dataflow("embedding-out")
input = aoop.input("input", flow, ...)
embedded = aoop.generate_embeddings("embedding_op", input, config)
aoop.output("output", embedded, ...)
```
"""

import logging
import os
from typing import Any, Dict, Optional

from openai import AzureOpenAI

import bytewax.operators as op
from bytewax.azure_ai_search import AzureSearchSink
from bytewax.dataflow import Stream, operator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AzureOpenAIConfig:
    """Configuration class for Azure OpenAI integration.

    This class handles the configuration and setup of the Azure OpenAI client
    for generating embeddings. It provides defaults based on environment variables
    but allows for direct parameter passing.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        service_name: Optional[str] = None,
        deployment_name: Optional[str] = None,
        dimensions: int = 1536,
    ) -> None:
        """Initialize the AzureOpenAIConfig with provided or environment credentials.

        Args:
            api_key (Optional[str]): Azure OpenAI API key.
            service_name (Optional[str]): Azure OpenAI service name.
            deployment_name (Optional[str]): Azure OpenAI deployment name.
            dimensions (int): The dimensions of the embedding. Default is 1536.
        """
        self.api_key = api_key or os.getenv("AZURE_OPENAI_API_KEY")
        self.service_name = service_name or os.getenv("AZURE_OPENAI_SERVICE")
        self.deployment_name = deployment_name or os.getenv(
            "AZURE_EMBEDDING_DEPLOYMENT_NAME"
        )
        self.dimensions = dimensions

        if not self.api_key or not self.service_name:
            error_message = "Azure OpenAI API key and service name must be provided."
            raise ValueError(error_message)
        self.client = AzureOpenAI(
            api_key=self.api_key,
            api_version="2023-10-01-preview",
            azure_endpoint=f"https://{self.service_name}.openai.azure.com/",
        )


@operator
def generate_embeddings(
    step_id: str,
    up: Stream[Dict[str, Any]],
    config: AzureOpenAIConfig,
) -> Stream[Dict[str, Any]]:
    """Operator to generate embeddings for each item in the stream using Azure OpenAI.

    Args:
        step_id (str): Unique ID for the operator.
        up (Stream[Dict[str, Any]]): Input stream of data items.
        config (AzureOpenAIConfig): Configuration object for Azure OpenAI.

    Returns:
        Stream[Dict[str, Any]]: Output stream with embeddings added to each item.
    """

    def generate_embedding(item: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # Generate embedding
            model_name = config.deployment_name or "text-embedding-ada-002"
            embedding_response = config.client.embeddings.create(
                input=item["text"], model=model_name
            )
            embedding = (
                embedding_response.data[0].embedding
                if embedding_response.data and embedding_response.data[0].embedding
                else [0.0] * config.dimensions
            )

            # Check embedding dimension
            if len(embedding) != config.dimensions:
                error_message = "Invalid embedding size: expected {}, got {}".format(
                    config.dimensions, len(embedding)
                )
                raise ValueError(error_message)
            # Add embedding to the item
            item["vector"] = embedding
            return item
        except Exception as e:
            logger.error(f"Failed to generate embeddings for item {item['id']}: {e}")
            return item

    # Use the map operator to apply the embedding function to each item in the stream
    return op.map(step_id, up, generate_embedding)


@operator
def output(step_id: str, up: Stream[Dict[str, Any]], sink: AzureSearchSink) -> None:
    """Output operator for writing the stream of items to the provided sink.

    Args:
        step_id (str): Unique ID for the operator.
        up (Stream[Dict[str, Any]]): Input stream of data items with embeddings.
        sink (AzureSearchSink): Sink to output data to.
    """
    return op.output(step_id, up, sink)
