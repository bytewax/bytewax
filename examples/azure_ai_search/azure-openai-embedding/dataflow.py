from openai import AzureOpenAI

from bytewax.azure_ai_search import AzureSearchSink
from bytewax.inputs import Source
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

from news_dataclasses import News, Review, Social

import os
from dotenv import load_dotenv

load_dotenv(override=True)
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
service_name = os.getenv("AZURE_SEARCH_SERVICE")
api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
DIMENSIONS = 1536
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_SERVICE = os.getenv("AZURE_OPENAI_SERVICE")
AZURE_EMBEDDING_DEPLOYMENT_NAME = os.getenv("AZURE_EMBEDDING_DEPLOYMENT_NAME")
AZURE_EMBEDDING_MODEL_NAME = os.getenv("AZURE_EMBEDDING_MODEL_NAME")

# Initialize Azure OpenAI client
client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    api_version="2023-10-01-preview",
    azure_endpoint=f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com/",
)


# Function to generate embeddings and return the updated event
def generate_embeddings(event):
    try:
        # Generate the embedding for the event's text
        embedding_response = client.embeddings.create(
            input=event.text, model=AZURE_EMBEDDING_DEPLOYMENT_NAME
        )

        # Extract the embedding and ensure it's valid
        embedding = (
            embedding_response.data[0].embedding
            if embedding_response.data and embedding_response.data[0].embedding
            else [0.0] * DIMENSIONS
        )

        if len(embedding) != DIMENSIONS:
            raise ValueError(
                f"Invalid embedding size: expected {DIMENSIONS}, got {len(embedding)}"
            )

        event_dict = {
            "id": event.id,
            "category": event.category,
            "text": event.text,
            "vector": embedding,
        }

        return event_dict
    except Exception as e:
        logger.error(f"Failed to generate embeddings: {e}")
        return None


# Mapping function to return a tuple for processing
def news_event(event):
    return event


# List of events
news_event_entries = [
    News(
        id="n1", category="news", text="Global markets rally on positive economic data."
    ),
    Review(
        id="r1",
        category="review",
        text="The camera quality of this phone is fantastic!",
    ),
    Social(
        id="s1",
        category="social",
        text="Just had the best coffee at this new place downtown.",
    ),
    News(
        id="n2",
        category="news",
        text="The election results are expected to be announced tomorrow.",
    ),
    Review(
        id="r2",
        category="review",
        text="Battery life could be better, but overall a decent phone.",
    ),
    Social(
        id="s2",
        category="social",
        text="Can't believe how beautiful the sunset was today!",
    ),
]

# Create and configure the Dataflow
flow = Dataflow("search_ctr")

# Input source for dataflow
inp = op.input("inp", flow, TestingSource(news_event_entries))

# Map the events to their ID and content
user_event_map = op.map("user_event", inp, news_event)

# Apply the embedding to each event's text
apply_embedding = op.map("apply_embedding", user_event_map, generate_embeddings)

# Define the schema for the Azure Search index
schema = {
    "id": {"type": "string", "default": None},
    "category": {"type": "string", "default": None},
    "text": {"type": "string", "default": None},
    "vector": {"type": "collection", "item_type": "single", "default": []},
}

# Initialize the AzureSearchSink with the schema
azure_sink = AzureSearchSink(
    azure_search_service=service_name,
    index_name="bytewax-index-openai",
    search_api_version="2024-07-01",
    search_admin_key=api_key,
    schema=schema,
)

# Output the final events with embeddings
op.output("out", apply_embedding, azure_sink)
