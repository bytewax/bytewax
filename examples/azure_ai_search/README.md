# Building embeddings in real time with Bytewax

## Example 1: Building your own data classes with small text entries

Code can be found in the `azure-openai-embedding` folder.

In this example you can find three files

* `connection.py` - execute it once to establish a connection to your Azure AI Search and Azure OpenAI services, and define a schema
* `news_dataclasses.py` - contains dataclass definitions for data containing short text to represent social media, news and review events
* `dataflow.py` - contains a complete Bytewax dataflow to initialize sample entries matching the data classes, applies an embedding model hosted on Azure OpenAI services, and stores the vectors in Azure AI Search through its Bytewax sink.

### Execution

To configure the connection and schema.


```bash
python connection.py
```

To generate embeddings and populate the Azure AI Search instance through the Bytewax sink.

```bash
python -m bytewax.run dataflow:flow
```


## Example 2: Handling complex pipelines to extract and embed news from the internet

Code can be found in the `haystack-orchestrator/` folder.

This example is based on code used in our blog ["Real time RAG with Bytewax and Haystack 2.0"](https://bytewax.io/blog/real-time-rag-with-bytewax-and-haystack-2-0)

In this example you can find three files:

* `connection.py` - execute it once to establish a connection to your Azure AI Search and Azure OpenAI services, and define a schema
* `indexing.py` - contains Haystack custom components to parse, extract content and generate embeddings from urls in a JSONL file in [here](./data/news_out.jsonl)
* `dataflow.py` - contains a complete Bytewax dataflow to parse the entries in the JSONL dataset, apply the custom component as a map operator step, and store the vectors in your Azure AI Search instante through the bytewax-azure-ai-search sink.

### Execution

To configure the connection and schema.


```bash
python connection.py
```

To generate embeddings and populate the Azure AI Search instance through the Bytewax sink.

```bash
python -m bytewax.run dataflow:flow
```
