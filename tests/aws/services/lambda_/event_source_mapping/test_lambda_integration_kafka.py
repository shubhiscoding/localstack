import json

import pytest
from localstack_snapshot.snapshots.transformer import KeyValueBasedTransformer, RegexTransformer

from localstack.aws.api.lambda_ import Runtime
from localstack.testing.aws.lambda_utils import (
    _await_event_source_mapping_enabled,
    _get_lambda_invocation_events,
)
from localstack.testing.pytest import markers
from localstack.utils.strings import short_uid
from tests.aws.services.lambda_.test_lambda import TEST_LAMBDA_PYTHON_ECHO


@pytest.fixture(autouse=True)
def _snapshot_transformers(snapshot):
    snapshot.add_transformer(snapshot.transform.resource_name())
    snapshot.add_transformer(
        KeyValueBasedTransformer(
            lambda k, v: str(v) if k == "timestamp" else None,
            "<timestamp>",
            replace_reference=False,
        )
    )
    snapshot.add_transformer(
        KeyValueBasedTransformer(
            lambda k, v: str(v) if k == "offset" else None,
            "<offset>",
            replace_reference=False,
        )
    )


@pytest.fixture
def kafka_broker_endpoint():
    """Returns the Kafka bootstrap server endpoint.
    Override this fixture to point to your Kafka broker.
    For local testing, a Docker Kafka broker on localhost:9092 is expected."""
    return "localhost:9092"


@pytest.fixture
def kafka_create_topic(kafka_broker_endpoint):
    """Factory fixture to create Kafka topics with automatic cleanup."""
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = None
    topics_created = []

    def _create_topic(topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        nonlocal admin
        if admin is None:
            admin = KafkaAdminClient(bootstrap_servers=kafka_broker_endpoint)
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        admin.create_topics([topic])
        topics_created.append(topic_name)
        return topic_name

    yield _create_topic

    if admin:
        try:
            admin.delete_topics(topics_created)
        except Exception:
            pass
        admin.close()


@pytest.fixture
def kafka_produce_messages(kafka_broker_endpoint):
    """Factory fixture to produce messages to a Kafka topic."""
    from kafka import KafkaProducer

    producer = None

    def _produce(topic: str, messages: list[dict]):
        nonlocal producer
        if producer is None:
            producer = KafkaProducer(
                bootstrap_servers=kafka_broker_endpoint,
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                value_serializer=lambda v: v.encode("utf-8") if v else None,
            )
        for msg in messages:
            producer.send(topic, key=msg.get("key"), value=msg.get("value"))
        producer.flush()

    yield _produce

    if producer:
        producer.close()


class TestKafkaSource:
    @markers.aws.validated
    def test_kafka_esm_basic_invoke(
        self,
        create_lambda_function,
        lambda_su_role,
        kafka_broker_endpoint,
        kafka_create_topic,
        kafka_produce_messages,
        cleanups,
        snapshot,
        aws_client,
    ):
        """Test that a Kafka ESM correctly invokes a Lambda with the expected event format."""
        function_name = f"lambda-kafka-{short_uid()}"
        topic_name = f"test-topic-{short_uid()}"
        # Kafka topics have no ARN, so resource_name() can't discover them.
        # Register a regex replacement like conftest does for account_id/region.
        snapshot.add_transformer(RegexTransformer(topic_name, "<topic>"))

        # 1. Create Lambda function (echo handler prints event to logs)
        create_lambda_function(
            func_name=function_name,
            handler_file=TEST_LAMBDA_PYTHON_ECHO,
            runtime=Runtime.python3_12,
            role=lambda_su_role,
        )

        # 2. Create Kafka topic
        kafka_create_topic(topic_name)

        # 3. Create event source mapping for self-managed Kafka
        create_esm_response = aws_client.lambda_.create_event_source_mapping(
            FunctionName=function_name,
            Topics=[topic_name],
            SourceAccessConfigurations=[
                {"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:000000000000:secret:dummy"}
            ],
            SelfManagedEventSource={
                "Endpoints": {
                    "KAFKA_BOOTSTRAP_SERVERS": [kafka_broker_endpoint],
                }
            },
            StartingPosition="TRIM_HORIZON",
            BatchSize=1,
            MaximumBatchingWindowInSeconds=0,
        )
        snapshot.match("create_esm_response", create_esm_response)
        mapping_uuid = create_esm_response["UUID"]
        cleanups.append(lambda: aws_client.lambda_.delete_event_source_mapping(UUID=mapping_uuid))
        _await_event_source_mapping_enabled(aws_client.lambda_, mapping_uuid)

        # 4. Produce a message to the topic
        kafka_produce_messages(
            topic_name,
            [{"key": "test-key", "value": json.dumps({"hello": "world"})}],
        )

        # 5. Wait for Lambda to be invoked and check the event
        events = _get_lambda_invocation_events(
            aws_client.logs, function_name, expected_num_events=1, retries=10
        )
        record = events[0]
        snapshot.match("kafka_event", record)

    @markers.aws.validated
    def test_kafka_esm_multiple_records_batch(
        self,
        create_lambda_function,
        lambda_su_role,
        kafka_broker_endpoint,
        kafka_create_topic,
        kafka_produce_messages,
        cleanups,
        snapshot,
        aws_client,
    ):
        """Test that a Kafka ESM batches multiple records into a single Lambda invocation."""
        function_name = f"lambda-kafka-{short_uid()}"
        topic_name = f"test-topic-{short_uid()}"
        snapshot.add_transformer(RegexTransformer(topic_name, "<topic>"))

        create_lambda_function(
            func_name=function_name,
            handler_file=TEST_LAMBDA_PYTHON_ECHO,
            runtime=Runtime.python3_12,
            role=lambda_su_role,
        )

        kafka_create_topic(topic_name)

        create_esm_response = aws_client.lambda_.create_event_source_mapping(
            FunctionName=function_name,
            Topics=[topic_name],
            SourceAccessConfigurations=[
                {"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:000000000000:secret:dummy"}
            ],
            SelfManagedEventSource={
                "Endpoints": {
                    "KAFKA_BOOTSTRAP_SERVERS": [kafka_broker_endpoint],
                }
            },
            StartingPosition="TRIM_HORIZON",
            BatchSize=10,
            MaximumBatchingWindowInSeconds=1,
        )
        mapping_uuid = create_esm_response["UUID"]
        cleanups.append(lambda: aws_client.lambda_.delete_event_source_mapping(UUID=mapping_uuid))
        _await_event_source_mapping_enabled(aws_client.lambda_, mapping_uuid)

        # Produce multiple messages
        messages = [
            {"key": f"key-{i}", "value": json.dumps({"message": f"hello-{i}"})}
            for i in range(5)
        ]
        kafka_produce_messages(topic_name, messages)

        # Wait for Lambda invocation
        events = _get_lambda_invocation_events(
            aws_client.logs, function_name, expected_num_events=1, retries=10
        )
        record = events[0]
        snapshot.match("kafka_batch_event", record)

    @markers.aws.validated
    def test_kafka_esm_filter_criteria(
        self,
        create_lambda_function,
        lambda_su_role,
        kafka_broker_endpoint,
        kafka_create_topic,
        kafka_produce_messages,
        cleanups,
        snapshot,
        aws_client,
    ):
        """Test that FilterCriteria correctly filters Kafka records before invoking Lambda."""
        function_name = f"lambda-kafka-{short_uid()}"
        topic_name = f"test-topic-{short_uid()}"
        snapshot.add_transformer(RegexTransformer(topic_name, "<topic>"))

        create_lambda_function(
            func_name=function_name,
            handler_file=TEST_LAMBDA_PYTHON_ECHO,
            runtime=Runtime.python3_12,
            role=lambda_su_role,
        )

        kafka_create_topic(topic_name)

        # Only accept records where the JSON value has "status" == "ready"
        create_esm_response = aws_client.lambda_.create_event_source_mapping(
            FunctionName=function_name,
            Topics=[topic_name],
            SourceAccessConfigurations=[
                {"Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:000000000000:secret:dummy"}
            ],
            SelfManagedEventSource={
                "Endpoints": {
                    "KAFKA_BOOTSTRAP_SERVERS": [kafka_broker_endpoint],
                }
            },
            StartingPosition="TRIM_HORIZON",
            BatchSize=10,
            MaximumBatchingWindowInSeconds=1,
            FilterCriteria={
                "Filters": [
                    {"Pattern": json.dumps({"value": {"status": ["ready"]}})}
                ]
            },
        )
        mapping_uuid = create_esm_response["UUID"]
        cleanups.append(lambda: aws_client.lambda_.delete_event_source_mapping(UUID=mapping_uuid))
        _await_event_source_mapping_enabled(aws_client.lambda_, mapping_uuid)

        # Produce messages: only the one with "status": "ready" should pass the filter
        kafka_produce_messages(
            topic_name,
            [
                {"key": "k1", "value": json.dumps({"status": "pending"})},
                {"key": "k2", "value": json.dumps({"status": "ready"})},
                {"key": "k3", "value": json.dumps({"status": "failed"})},
            ],
        )

        # Only the "status": "ready" message should reach Lambda
        events = _get_lambda_invocation_events(
            aws_client.logs, function_name, expected_num_events=1, retries=10
        )
        record = events[0]
        snapshot.match("kafka_filtered_event", record)
