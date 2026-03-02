"""Unit tests for Kafka connectors using mocks (no broker required)."""

from unittest.mock import MagicMock, call, patch

import pytest
from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage, KafkaSource, _KafkaSinkPartition


class TestKafkaSinkPartitionWriteBatch:
    """Tests for _KafkaSinkPartition.write_batch() — issue #522."""

    def test_write_batch_normal(self):
        """Normal produce calls succeed without error."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
        ]
        partition.write_batch(msgs)

        assert producer.produce.call_count == 2
        assert producer.poll.call_count == 2
        producer.flush.assert_called_once()

    def test_write_batch_buffer_error_retry(self):
        """BufferError triggers flush then successful retry."""
        producer = MagicMock()
        producer.produce.side_effect = [BufferError("Local: Queue full"), None]
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        partition.write_batch(msgs)

        # First produce fails, flush, second produce succeeds
        assert producer.produce.call_count == 2
        assert producer.flush.call_count == 2  # 1 recovery + 1 end-of-batch

    def test_write_batch_buffer_error_multiple_messages(self):
        """BufferError on one message doesn't block other messages."""
        producer = MagicMock()
        # msg1 ok, msg2 fails then retries ok, msg3 ok
        producer.produce.side_effect = [None, BufferError("full"), None, None]
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
            KafkaSinkMessage(key=b"k3", value=b"v3"),
        ]
        partition.write_batch(msgs)

        assert producer.produce.call_count == 4  # 3 normal + 1 retry
        assert producer.poll.call_count == 3

    def test_write_batch_no_topic_raises(self):
        """RuntimeError when no topic set on message or partition."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, None)

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        with pytest.raises(RuntimeError, match="No topic to produce to"):
            partition.write_batch(msgs)

    def test_write_batch_message_topic_override(self):
        """Per-message topic overrides the partition default."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, "default-topic")

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1", topic="override-topic")]
        partition.write_batch(msgs)

        producer.produce.assert_called_once_with(
            value=b"v1",
            key=b"k1",
            headers=[],
            topic="override-topic",
            timestamp=0,
        )


class TestKafkaSourceListParts:
    """Tests for KafkaSource.list_parts() — issues #541 and #377."""

    @patch("bytewax.connectors.kafka._list_parts", return_value=["0-test-topic"])
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_calls_poll(self, mock_admin_cls, mock_list_parts):
        """AdminClient.poll(0) is called before _list_parts (issue #541)."""
        mock_client = MagicMock()
        mock_admin_cls.return_value = mock_client

        source = KafkaSource(["localhost:9092"], ["test-topic"])
        source.list_parts()

        mock_client.poll.assert_called_once_with(0)
        mock_list_parts.assert_called_once_with(mock_client, ["test-topic"])

    @patch("bytewax.connectors.kafka._list_parts", return_value=["0-test-topic"])
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_strips_group_id(self, mock_admin_cls, mock_list_parts):
        """group.id is stripped from AdminClient config (issue #377)."""
        mock_admin_cls.return_value = MagicMock()

        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            add_config={"group.id": "my-group", "security.protocol": "SASL_SSL"},
        )
        source.list_parts()

        created_config = mock_admin_cls.call_args[0][0]
        assert "group.id" not in created_config
        assert created_config["security.protocol"] == "SASL_SSL"

    @patch("bytewax.connectors.kafka._list_parts", return_value=["0-test-topic"])
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_passes_connection_config(self, mock_admin_cls, mock_list_parts):
        """Security and connection config passes through to AdminClient."""
        mock_admin_cls.return_value = MagicMock()

        source = KafkaSource(
            ["broker1:9092", "broker2:9092"],
            ["test-topic"],
            add_config={"security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN"},
        )
        source.list_parts()

        created_config = mock_admin_cls.call_args[0][0]
        assert created_config["bootstrap.servers"] == "broker1:9092,broker2:9092"
        assert created_config["security.protocol"] == "SASL_SSL"
        assert created_config["sasl.mechanism"] == "PLAIN"


class TestKafkaSourceBuildPart:
    """Tests for KafkaSource.build_part() — issue #377."""

    @patch("bytewax.connectors.kafka._KafkaSourcePartition")
    def test_build_part_ignores_user_group_id(self, mock_partition_cls):
        """User group.id is overridden to BYTEWAX_IGNORED (issue #377)."""
        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            add_config={"group.id": "my-consumer-group"},
        )
        source.build_part("step-1", "0-test-topic", None)

        config = mock_partition_cls.call_args[0][1]
        assert config["group.id"] == "BYTEWAX_IGNORED"
        assert config["enable.auto.commit"] == "false"

    @patch("bytewax.connectors.kafka._KafkaSourcePartition")
    def test_build_part_preserves_other_config(self, mock_partition_cls):
        """Non-group config from add_config is preserved."""
        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            add_config={"security.protocol": "SASL_SSL", "group.id": "ignored"},
        )
        source.build_part("step-1", "0-test-topic", None)

        config = mock_partition_cls.call_args[0][1]
        assert config["security.protocol"] == "SASL_SSL"
        assert config["group.id"] == "BYTEWAX_IGNORED"


class TestKafkaSinkBuild:
    """Tests for KafkaSink.build() — issue #377."""

    @patch("bytewax.connectors.kafka.Producer")
    def test_kafka_sink_strips_group_id(self, mock_producer_cls):
        """group.id is stripped from Producer config (issue #377)."""
        mock_producer_cls.return_value = MagicMock()

        sink = KafkaSink(
            ["localhost:9092"],
            "test-topic",
            add_config={"group.id": "my-group", "linger.ms": "100"},
        )
        sink.build("step-1", 0, 1)

        created_config = mock_producer_cls.call_args[0][0]
        assert "group.id" not in created_config
        assert created_config["linger.ms"] == "100"
