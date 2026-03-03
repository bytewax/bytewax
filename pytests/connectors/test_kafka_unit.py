"""Unit tests for Kafka connector fixes (no broker required)."""

from unittest.mock import MagicMock, patch

import pytest
from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    _KafkaSinkPartition,
)


class TestKafkaSinkBufferError:
    """Tests for Fix #522: KafkaSink should handle BufferError gracefully."""

    def test_write_batch_normal(self):
        """Normal produce calls work without error."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
        ]
        partition.write_batch(msgs)

        assert producer.produce.call_count == 2
        assert producer.poll.call_count == 2
        producer.flush.assert_called()

    def test_write_batch_buffer_error_retry(self):
        """BufferError triggers flush then retry."""
        producer = MagicMock()
        # First produce raises BufferError, second (retry) succeeds
        producer.produce.side_effect = [BufferError("queue full"), None]
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        partition.write_batch(msgs)

        # produce called twice: initial + retry
        assert producer.produce.call_count == 2
        # flush called: once for BufferError recovery + once at end
        assert producer.flush.call_count == 2

    def test_write_batch_buffer_error_multiple_messages(self):
        """BufferError on one message doesn't affect others."""
        producer = MagicMock()
        # First msg: produce OK, second msg: BufferError then OK, third msg: OK
        producer.produce.side_effect = [None, BufferError("queue full"), None, None]
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
            KafkaSinkMessage(key=b"k3", value=b"v3"),
        ]
        partition.write_batch(msgs)

        # 3 initial + 1 retry = 4
        assert producer.produce.call_count == 4
        assert producer.poll.call_count == 3

    def test_write_batch_with_explicit_topic(self):
        """Messages with explicit topic override default."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, "default-topic")

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1", topic="custom-topic")]
        partition.write_batch(msgs)

        producer.produce.assert_called_once_with(
            value=b"v1",
            key=b"k1",
            headers=[],
            topic="custom-topic",
            timestamp=0,
        )

    def test_write_batch_no_topic_raises(self):
        """Missing topic raises RuntimeError."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, None)

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        with pytest.raises(RuntimeError, match="No topic"):
            partition.write_batch(msgs)


class TestKafkaSourceOAuthPoll:
    """Tests for Fix #541: KafkaSource.list_parts() should poll for OAUTHBEARER."""

    @patch("bytewax.connectors.kafka.AdminClient")
    @patch("bytewax.connectors.kafka._list_parts")
    def test_list_parts_calls_poll_before_list(self, mock_list_parts, mock_admin_cls):
        """poll(0) is called on AdminClient before _list_parts."""
        mock_client = MagicMock()
        mock_admin_cls.return_value = mock_client
        mock_list_parts.return_value = ["0-test-topic"]

        source = KafkaSource(["localhost:9092"], ["test-topic"], tail=False)
        parts = source.list_parts()

        mock_admin_cls.assert_called_once()
        mock_client.poll.assert_called_once_with(0)
        mock_list_parts.assert_called_once_with(mock_client, ["test-topic"])
        assert parts == ["0-test-topic"]

    @patch("bytewax.connectors.kafka.AdminClient")
    @patch("bytewax.connectors.kafka._list_parts")
    def test_list_parts_passes_add_config(self, mock_list_parts, mock_admin_cls):
        """add_config is forwarded to AdminClient."""
        mock_client = MagicMock()
        mock_admin_cls.return_value = mock_client
        mock_list_parts.return_value = []

        add_config = {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
        }
        source = KafkaSource(
            ["broker1:9092"], ["topic1"], tail=False, add_config=add_config
        )
        source.list_parts()

        call_config = mock_admin_cls.call_args[0][0]
        assert call_config["security.protocol"] == "SASL_SSL"
        assert call_config["sasl.mechanisms"] == "OAUTHBEARER"
        assert call_config["bootstrap.servers"] == "broker1:9092"


class TestKafkaGroupIdLeak:
    """Tests for Fix #377: group.id should not leak to AdminClient/Producer."""

    @patch("bytewax.connectors.kafka._list_parts", return_value=["0-test-topic"])
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_strips_group_id(self, mock_admin_cls, mock_list_parts):
        """group.id is stripped from AdminClient config."""
        mock_admin_cls.return_value = MagicMock()

        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            tail=False,
            add_config={"group.id": "my-group", "security.protocol": "SASL_SSL"},
        )
        source.list_parts()

        created_config = mock_admin_cls.call_args[0][0]
        assert "group.id" not in created_config
        assert created_config["security.protocol"] == "SASL_SSL"

    @patch("bytewax.connectors.kafka._KafkaSourcePartition")
    def test_build_part_ignores_user_group_id(self, mock_partition_cls):
        """User group.id is overridden to BYTEWAX_IGNORED."""
        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            tail=False,
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
            tail=False,
            add_config={"security.protocol": "SASL_SSL", "group.id": "ignored"},
        )
        source.build_part("step-1", "0-test-topic", None)

        config = mock_partition_cls.call_args[0][1]
        assert config["security.protocol"] == "SASL_SSL"
        assert config["group.id"] == "BYTEWAX_IGNORED"

    @patch("bytewax.connectors.kafka._KafkaSourcePartition")
    def test_build_part_enforces_auto_commit_false(self, mock_partition_cls):
        """enable.auto.commit=false cannot be overridden by user config."""
        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            tail=False,
            add_config={"enable.auto.commit": "true"},
        )
        source.build_part("step-1", "0-test-topic", None)

        config = mock_partition_cls.call_args[0][1]
        assert config["enable.auto.commit"] == "false"

    @patch("bytewax.connectors.kafka.Producer")
    def test_kafka_sink_strips_group_id(self, mock_producer_cls):
        """group.id is stripped from Producer config."""
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
