# from typing import Any, Dict
# from unittest.mock import patch

# from bytewax.azure_ai_search import AzureSearchSink, _AzureSearchPartition

# # Sample schema and data
# schema: Dict[str, Dict[str, Any]] = {
#     "id": {"type": "string", "default": None},
#     "content": {"type": "string", "default": None},
#     "meta": {"type": "string", "default": None},
#     "vector": {"type": "collection", "default": []},
# }

# sample_data = [
#     {
#         "id": "123",
#         "content": "This is a test document",
#         "meta": "Test metadata",
#         "vector": [0.1, 0.2, 0.3],
#     }
# ]


# def test_azure_search_partition_write_batch_success() -> None:
#     """Test successful batch write to Azure Search"""
#     # Mock requests.post to simulate a successful API call
#     with patch("requests.post") as mock_post:
#         mock_post.return_value.status_code = 200

#         partition = _AzureSearchPartition(
#             azure_search_service="test-service",
#             index_name="test-index",
#             search_api_version="2024-07-01",
#             search_admin_key="test-key",
#             schema=schema,
#         )

#         result = partition.write_batch(sample_data)

#         # Make sure to handle None cases if they can exist
#         assert result is not None
#         assert result["status"] == "success"
#         mock_post.assert_called_once()


# def test_azure_search_partition_write_batch_failure() -> None:
#     """Test failed batch write to Azure Search"""
#     # Mock requests.post to simulate a failed API call
#     with patch("requests.post") as mock_post:
#         mock_post.return_value.status_code = 400
#         mock_post.return_value.text = "Bad Request"

#         partition = _AzureSearchPartition(
#             azure_search_service="test-service",
#             index_name="test-index",
#             search_api_version="2024-07-01",
#             search_admin_key="test-key",
#             schema=schema,
#         )

#         result = partition.write_batch(sample_data)

#         # Make sure to handle None cases if they can exist
#         assert result is not None
#         assert result["status"] == "Bad Request"
#         mock_post.assert_called_once()


# def test_azure_search_sink_build() -> None:
#     """Test the AzureSearchSink build method"""
#     sink = AzureSearchSink(
#         azure_search_service="test-service",
#         index_name="test-index",
#         search_api_version="2024-07-01",
#         search_admin_key="test-key",
#         schema=schema,
#     )

#     partition = sink.build(step_id="step1", worker_index=0, worker_count=1)

#     assert isinstance(partition, _AzureSearchPartition)
#     assert partition.azure_search_service == "test-service"
#     assert partition.index_name == "test-index"
#     assert partition.search_api_version == "2024-07-01"
#     assert partition.search_admin_key == "test-key"
#     assert partition.schema == schema
