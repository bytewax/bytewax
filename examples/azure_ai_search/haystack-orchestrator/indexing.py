from haystack import Pipeline
from haystack.components.embedders import OpenAIDocumentEmbedder
from haystack.components.preprocessors import DocumentCleaner
from haystack.components.preprocessors import DocumentSplitter
from pathlib import Path
from haystack.utils import Secret
from haystack.components.fetchers import LinkContentFetcher
from haystack.components.converters import HTMLToDocument

from haystack import component, Document
from typing import Any, Dict, List, Union
from haystack.dataclasses import ByteStream
from dotenv import load_dotenv
import os
import json
import numpy as np

load_dotenv(".env")
open_ai_key = os.environ.get("OPENAI_API_KEY")


class JSONLReader:
    def __init__(self, metadata_fields=None, open_ai_key=None):
        """
        Initialize the JSONLReader with optional metadata fields and a link keyword.

        :param metadata_fields: List of fields in the JSONL to retain as metadata.
        :param open_ai_key: OpenAI API key for embedding content.
        """
        self.metadata_fields = metadata_fields or []

        # Set up cleaning mechanism
        regex_pattern = r"(?i)\bloading\s*\.*\s*|(\s*--\s*-\s*)+"

        fetcher = LinkContentFetcher(retry_attempts=3, timeout=10)
        converter = HTMLToDocument()
        document_cleaner = DocumentCleaner(
            remove_empty_lines=True,
            remove_extra_whitespaces=True,
            remove_repeated_substrings=False,
            remove_substrings=None,
            remove_regex=regex_pattern,
        )

        document_splitter = DocumentSplitter(split_by="passage")
        document_embedder = OpenAIDocumentEmbedder(
            api_key=Secret.from_token(open_ai_key)
        )

        # Initialize pipeline
        self.pipeline = Pipeline()

        # Add components
        self.pipeline.add_component("fetcher", fetcher)
        self.pipeline.add_component("converter", converter)
        self.pipeline.add_component("cleaner", document_cleaner)
        self.pipeline.add_component("splitter", document_splitter)
        self.pipeline.add_component("embedder", document_embedder)

        # Connect components
        self.pipeline.connect("fetcher", "converter")
        self.pipeline.connect("converter", "cleaner")
        self.pipeline.connect("cleaner", "splitter")
        self.pipeline.connect("splitter", "embedder")

    @component.output_types(documents=List[Document])
    def run(self, event: List[Union[str, Path, ByteStream]]):
        """
        Process each source file, read URLs and their associated metadata,
        fetch HTML content using a pipeline, and convert to Haystack Documents.
        :param sources: File paths or ByteStreams to process.
        :return: A list of Haystack Documents.
        """

        # Extract URL and modify it if necessary
        url = event.get("url")
        if url and "-index.html" in url:
            url = url.replace("-index.html", ".txt")

        # else:
        metadata = {
            field: event.get(field) for field in self.metadata_fields if field in event
        }
        # Assume a pipeline fetches and processes this URL
        doc = self.pipeline.run({"fetcher": {"urls": [url]}})
        document_obj = doc["embedder"]["documents"][0]
        content = document_obj.content
        additional_metadata = document_obj.meta

        embedding = document_obj.embedding

        # Safely access the embedding metadata
        embedding_metadata = doc.get("embedder", {}).get("meta", {})
        metadata.update(embedding_metadata)
        document = Document(
            id=document_obj.id, content=content, meta=metadata, embedding=embedding
        )

        metadata.update(additional_metadata)

        return document

    def document_to_dict(self, document: Document) -> Dict:
        """
        Convert a Haystack Document object to a dictionary.

        :param document: A Haystack Document object.
        :return: A dictionary representation of the document.
        """
        embedding = document.embedding

        # Convert embedding to a list of floats
        if embedding is not None:
            if isinstance(embedding, np.ndarray):
                embedding = embedding.tolist()
            elif isinstance(embedding, list):
                embedding = [float(x) for x in embedding]
            else:
                raise ValueError(
                    "Embedding is not in a recognized format (must be a NumPy array or list)."
                )

        # Log the first few values of the embedding for debugging
        print(f"Embedding: {embedding[:5]}")

        flattened_meta = self.flatten_meta(document.meta)

        return {
            "id": document.id,
            "content": document.content,
            "meta": json.dumps(flattened_meta),
            "vector": embedding,
        }

    def flatten_meta(self, meta):
        """
        Flatten a nested dictionary to a single-level dictionary.

        :param meta: Nested dictionary to flatten.
        :return: Flattened dictionary.
        """

        def _flatten(d, parent_key=""):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}_{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(_flatten(v, new_key).items())
                else:
                    items.append(
                        (
                            new_key,
                            str(v) if not isinstance(v, (str, int, float, bool)) else v,
                        )
                    )
            return dict(items)

        return _flatten(meta)
