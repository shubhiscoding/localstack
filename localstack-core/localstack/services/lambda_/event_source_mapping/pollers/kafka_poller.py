import base64
import json
import logging

from botocore.client import BaseClient
from kafka import KafkaConsumer
from localstack.services.lambda_.event_source_mapping.pollers.poller import (
    EmptyPollResultsException,
    Poller,
)
from localstack.services.lambda_.event_source_mapping.event_processor import (
    EventProcessor,
    PartialBatchFailureError,
)

LOG = logging.getLogger(__name__)

class KafkaPoller(Poller):
    _consumer: KafkaConsumer
    starting_position: str
    consumer_group_id: str
    maximum_batching_window: int
    batch_size: int
    topic_name: str
    bootstrap_servers: list[str]

    def __init__(
        self,
        bootstrap_servers: list[str],
        source_parameters: dict | None = None,
        source_client: BaseClient | None = None,
        processor: EventProcessor | None = None,
        aws_region: str | None = None,
    ):
        super().__init__(
            source_arn=None,
            source_parameters=source_parameters,
            source_client=source_client,
            processor=processor,
        )
        self.aws_region = aws_region
        self._consumer = None
        self.starting_position = self.source_parameters["SelfManagedKafkaParameters"].get("StartingPosition", "LATEST")
        self.consumer_group_id = self.source_parameters["SelfManagedKafkaParameters"].get("ConsumerGroupID", "default")
        self.maximum_batching_window = self.source_parameters["SelfManagedKafkaParameters"].get(
            "MaximumBatchingWindowInSeconds", 0
        )
        self.batch_size = self.source_parameters["SelfManagedKafkaParameters"].get("BatchSize", 100)
        self.topic_name = self.source_parameters["SelfManagedKafkaParameters"]["TopicName"]
        self.bootstrap_servers = bootstrap_servers

    def event_source(self) -> str:
        return "SelfManagedKafka"
    

    def _get_or_create_consumer(self) -> KafkaConsumer:
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                self.topic_name,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group_id,
                auto_offset_reset={"TRIM_HORIZON": "earliest", "LATEST": "latest"}.get(
                    self.starting_position, "latest"
                ),
                enable_auto_commit=False,
            )
        return self._consumer
    
    def poll_events(self):
        consumer = self._get_or_create_consumer()

        # 1. Poll Kafka — returns {TopicPartition: [ConsumerRecord, ...]}
        raw_records = consumer.poll(
            timeout_ms=self.maximum_batching_window * 1000,
            max_records=self.batch_size,
        )

        # 2. Nothing? Tell the worker to back off
        if not raw_records:
            raise EmptyPollResultsException(
                service="kafka",
                source_arn=f"{self.topic_name}@{','.join(self.bootstrap_servers)}",
            )

        # 3. Transform raw ConsumerRecords into a flat list of event dicts
        #    value/key are kept as decoded (parsed JSON or raw string) for filtering
        events = self._transform_records(raw_records)

        # 4. Apply FilterCriteria (no-op if no filters configured)
        matching_events = self.filter_events(events)

        # 5. If all records were filtered out, commit offsets and return early
        if not matching_events:
            consumer.commit()
            return

        # 6. Base64-encode key/value for the final AWS event format
        self._encode_fields(matching_events)

        # 7. Add metadata (eventSource, awsRegion) after filtering
        self.add_source_metadata(matching_events)

        # 8. Build the Kafka envelope grouping records by topic-partition
        envelope = self._build_kafka_envelope(matching_events)

        # 9. Hand off to the pipeline — THIS invokes Lambda
        #    On partial failure, don't commit — Kafka will redeliver from last committed offset
        try:
            self.processor.process_events_batch(envelope)
        except PartialBatchFailureError:
            return

        # 10. Only commit AFTER fully successful processing
        consumer.commit()

    def _transform_records(self, raw_records: dict) -> list[dict]:
        """Convert raw kafka-python ConsumerRecords into a flat list of event dicts.
        key/value are kept as decoded (parsed JSON or raw string) so that
        FilterCriteria can inspect them. They are base64-encoded later by _encode_fields."""
        events = []
        for topic_partition, records in raw_records.items():
            for record in records:
                event = {
                    "topic": topic_partition.topic,
                    "partition": topic_partition.partition,
                    "offset": record.offset,
                    "timestamp": record.timestamp,
                    "timestampType": "CREATE_TIME" if record.timestamp_type == 0 else "LOG_APPEND_TIME",
                    "key": record.key.decode("utf-8") if record.key else None,
                    "value": self._decode_value(record.value),
                    "headers": [
                        {h[0]: list(h[1]) if h[1] else []}
                        for h in (record.headers or [])
                    ],
                }
                events.append(event)
        return events

    @staticmethod
    def _decode_value(value: bytes | None):
        """Decode raw Kafka value bytes into parsed JSON (for filtering) or a raw string."""
        if not value:
            return None
        decoded = value.decode("utf-8")
        try:
            return json.loads(decoded)
        except json.JSONDecodeError:
            return decoded

    @staticmethod
    def _encode_fields(events: list[dict]):
        """Base64-encode key and value into the final AWS event format."""
        for event in events:
            if event.get("key"):
                event["key"] = base64.b64encode(event["key"].encode("utf-8")).decode("utf-8")
            if event.get("value") is not None:
                raw = event["value"]
                if not isinstance(raw, str):
                    raw = json.dumps(raw)
                event["value"] = base64.b64encode(raw.encode("utf-8")).decode("utf-8")

    def _build_kafka_envelope(self, events: list[dict]) -> dict:
        """Group event dicts by topic-partition into the AWS Kafka event envelope."""
        records_by_topic_partition = {}
        for event in events:
            key = f"{event['topic']}-{event['partition']}"
            records_by_topic_partition.setdefault(key, []).append(event)
        return {
            "eventSource": self.event_source(),
            "bootstrapServers": ",".join(self.bootstrap_servers),
            "records": records_by_topic_partition,
        }
    
    def close(self):
        if self._consumer:
            self._consumer.close()
            self._consumer = None
