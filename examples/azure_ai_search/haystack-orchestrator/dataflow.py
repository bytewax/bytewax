from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSource

from dotenv import load_dotenv
import os
import json

from indexing import JSONLReader
from bytewax.azure_ai_search import AzureSearchSink

load_dotenv(".env")
open_ai_key = os.environ.get("OPENAI_API_KEY")
service_name = os.getenv("AZURE_SEARCH_SERVICE")
api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
DIMENSIONS = 1536
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_SERVICE = os.getenv("AZURE_OPENAI_SERVICE")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_EMBEDDING_DEPLOYMENT_NAME = os.getenv("AZURE_EMBEDDING_DEPLOYMENT_NAME")
AZURE_EMBEDDING_MODEL_NAME = os.getenv("AZURE_EMBEDDING_MODEL_NAME")


def safe_deserialize(data):
    try:
        # Attempt to load the JSON data
        parsed_data = json.loads(data)

        # Check if the data is in the list format with a possible null as the first item
        if isinstance(parsed_data, list):
            if len(parsed_data) == 2 and (
                parsed_data[0] is None or isinstance(parsed_data[0], str)
            ):
                event = parsed_data[1]  # Select the dictionary which is the second item
            else:
                print(f"Skipping unexpected list format: {data}")
                return None
        elif isinstance(parsed_data, dict):
            # It's the simple dictionary format
            event = parsed_data
        else:
            print(f"Skipping unexpected data type: {data}")
            return None

        if "link" in event:
            event["url"] = event.pop("link")
        # Check for the presence of 'url' key to further validate the data
        if "url" in event:
            return event  # Return the entire event dict or adapt as needed
        else:
            print(f"Missing 'url' key in data: {data}")
            return None

    except json.JSONDecodeError as e:
        print(f"JSON decode error ({e}) for data: {data}")
        return None
    except Exception as e:
        print(f"Error processing data ({e}): {data}")
        return None


jsonl_reader = JSONLReader(
    metadata_fields=["symbols", "headline", "url"], open_ai_key=open_ai_key
)


def process_event(event):
    """Wrapper to handle the processing of each event."""
    if event:
        document = jsonl_reader.run(event)
        return jsonl_reader.document_to_dict(document)
    return None


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

flow = Dataflow("rag-pipeline")
input_data = op.input("input", flow, FileSource("data/news_out.jsonl"))
deserialize_data = op.map("deserialize", input_data, safe_deserialize)
extract_html = op.map("extract_html", deserialize_data, process_event)
op.output("output", extract_html, azure_sink)
