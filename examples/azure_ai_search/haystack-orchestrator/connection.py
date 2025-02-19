import os
from dotenv import load_dotenv
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient, SearchIndexerClient
from openai import AzureOpenAI

from azure.search.documents.indexes.models import (
    AzureOpenAIVectorizerParameters,
    AzureOpenAIVectorizer,
    ExhaustiveKnnParameters,
    ExhaustiveKnnAlgorithmConfiguration,
    HnswParameters,
    HnswAlgorithmConfiguration,
    SearchableField,
    SimpleField,
    SearchFieldDataType,
    SearchIndex,
    VectorSearch,
    VectorSearchAlgorithmKind,
    VectorSearchAlgorithmMetric,
    VectorSearchProfile,
    CorsOptions,
    ScoringProfile,
)

from typing import List

load_dotenv(".env")

# Load environment variables
endpoint = os.getenv("AZURE_SEARCH_SERVICE_ENDPOINT")
service_name = os.getenv("AZURE_SEARCH_SERVICE")
api_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
DIMENSIONS = 1536
AZURE_OPENAI_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_SERVICE = os.getenv("AZURE_OPENAI_SERVICE")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_EMBEDDING_DEPLOYMENT_NAME = os.getenv("AZURE_EMBEDDING_DEPLOYMENT_NAME")
AZURE_EMBEDDING_MODEL_NAME = os.getenv("AZURE_EMBEDDING_MODEL_NAME")


# Initialize Client
client = AzureOpenAI(
    api_key=AZURE_OPENAI_KEY,
    api_version="2023-10-01-preview",
    azure_endpoint=f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com/",
)
credential = AzureKeyCredential(api_key)

index_client = SearchIndexClient(endpoint, credential)
search_indexer_client = SearchIndexerClient(endpoint, credential)

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


# Configure the vector search configuration
vector_search = VectorSearch(
    algorithms=[
        HnswAlgorithmConfiguration(
            name="myHnsw",
            kind=VectorSearchAlgorithmKind.HNSW,
            parameters=HnswParameters(
                m=4,
                ef_construction=400,
                ef_search=500,
                metric=VectorSearchAlgorithmMetric.COSINE,
            ),
        ),
        ExhaustiveKnnAlgorithmConfiguration(
            name="myExhaustiveKnn",
            kind=VectorSearchAlgorithmKind.EXHAUSTIVE_KNN,
            parameters=ExhaustiveKnnParameters(
                metric=VectorSearchAlgorithmMetric.COSINE,
            ),
        ),
    ],
    profiles=[
        VectorSearchProfile(
            name="myHnswProfile",
            algorithm_configuration_name="myHnsw",
            vectorizer_name="myOpenAI",
        ),
        VectorSearchProfile(
            name="myExhaustiveKnnProfile",
            algorithm_configuration_name="myExhaustiveKnn",
            vectorizer_name="myOpenAI",
        ),
    ],
    vectorizers=[
        AzureOpenAIVectorizer(
            vectorizer_name="myOpenAI",
            kind="azureOpenAI",
            parameters=AzureOpenAIVectorizerParameters(
                resource_url=os.getenv("AZURE_OPENAI_ENDPOINT"),
                deployment_name=AZURE_EMBEDDING_DEPLOYMENT_NAME,
                api_key=os.getenv("AZURE_OPENAI_API_KEY"),
                model_name=AZURE_EMBEDDING_MODEL_NAME,
            ),
        ),
    ],
)


# Create the search index with the semantic settings
cors_options = CorsOptions(allowed_origins=["*"], max_age_in_seconds=60)
scoring_profiles: List[ScoringProfile] = []
index = SearchIndex(
    name="bytewax-index",
    fields=fields,
    vector_search=vector_search,
    scoring_profiles=scoring_profiles,
    cors_options=cors_options,
)
result = index_client.create_or_update_index(index)

print(f"Creating bytewax-index search index")
