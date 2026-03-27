"""
Kafka transaction producer with Avro schema validation and Schema Registry integration.
Produces synthetic financial transaction events for the streaming pipeline.
"""

import json
import logging
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = logging.getLogger(__name__)


TRANSACTION_SCHEMA = """
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.fintech.events",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "account_id",     "type": "string"},
    {"name": "amount_cents",   "type": "long"},
    {"name": "currency",       "type": "string"},
    {"name": "merchant_id",    "type": "string"},
    {"name": "merchant_category_code", "type": "string"},
    {"name": "event_type",     "type": "string"},
    {"name": "event_timestamp","type": "string"},
    {"name": "card_last_four", "type": "string"},
    {"name": "is_international","type": "boolean"},
    {"name": "metadata",       "type": {"type": "map", "values": "string"}, "default": {}}
  ]
}
"""


@dataclass
class TransactionEvent:
    transaction_id: str
    account_id: str
    amount_cents: int
    currency: str
    merchant_id: str
    merchant_category_code: str
    event_type: str
    event_timestamp: str
    card_last_four: str
    is_international: bool
    metadata: dict

    @classmethod
    def create(
        cls,
        account_id: str,
        amount_cents: int,
        merchant_id: str,
        merchant_category_code: str = "5411",
        event_type: str = "AUTHORIZATION",
        currency: str = "USD",
        card_last_four: str = "0000",
        is_international: bool = False,
        metadata: Optional[dict] = None,
    ) -> "TransactionEvent":
        return cls(
            transaction_id=str(uuid.uuid4()),
            account_id=account_id,
            amount_cents=amount_cents,
            currency=currency,
            merchant_id=merchant_id,
            merchant_category_code=merchant_category_code,
            event_type=event_type,
            event_timestamp=datetime.utcnow().isoformat(),
            card_last_four=card_last_four,
            is_international=is_international,
            metadata=metadata or {},
        )


class TransactionProducer:
    """
    Kafka producer for financial transaction events.
    Validates messages against Avro schema before producing.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        config: Optional[dict] = None,
    ):
        self.topic = topic
        self._schema_registry = SchemaRegistryClient({"url": schema_registry_url})
        self._serializer = AvroSerializer(
            self._schema_registry,
            TRANSACTION_SCHEMA,
            lambda event, ctx: asdict(event),
        )
        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "retry.backoff.ms": 300,
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.size": 65536,
            **(config or {}),
        }
        self._producer = Producer(producer_config)
        self._produced = 0
        self._errors = 0

    def produce(self, event: TransactionEvent) -> None:
        """Produce a single transaction event. Raises on serialization failure."""
        serialized = self._serializer(
            event,
            SerializationContext(self.topic, MessageField.VALUE),
        )
        self._producer.produce(
            topic=self.topic,
            key=event.account_id.encode("utf-8"),
            value=serialized,
            on_delivery=self._delivery_callback,
        )
        self._producer.poll(0)

    def produce_batch(self, events: list[TransactionEvent]) -> None:
        """Produce a batch of events. Flushes after all are queued."""
        for event in events:
            self.produce(event)
        self._producer.flush()
        logger.info("Flushed batch of %d events", len(events))

    def close(self) -> None:
        self._producer.flush()
        logger.info(
            "Producer closed. Produced: %d, Errors: %d",
            self._produced,
            self._errors,
        )

    def _delivery_callback(self, err, msg) -> None:
        if err:
            self._errors += 1
            logger.error("Delivery failed for key %s: %s", msg.key(), err)
        else:
            self._produced += 1
            if self._produced % 10_000 == 0:
                logger.info("Produced %d messages to %s", self._produced, msg.topic())

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
