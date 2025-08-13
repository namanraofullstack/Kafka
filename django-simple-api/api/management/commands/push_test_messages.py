import time
import random
import uuid
import json
from django.core.management.base import BaseCommand
from kafka import KafkaProducer
import os

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TEST_TOPIC", "test-topic")

class Command(BaseCommand):
    help = "Push randomized test messages to Kafka"

    def add_arguments(self, parser):
        parser.add_argument(
            "--count",
            type=int,
            default=10000,
            help="Number of test messages to send"
        )
        parser.add_argument(
            "--topic",
            type=str,
            default=TOPIC,
            help="Kafka topic to send messages to"
        )

    def handle(self, *args, **options):
        n = options["count"]
        topic = options["topic"]

        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        # Base templates (5 different messages)
        base_messages = [
            {
                "source_id": str(uuid.uuid4()),
                "collection_id": str(uuid.uuid4()),
                "data_id": str(uuid.uuid4()),
                "object_id": str(uuid.uuid4()),
                "hashes": {
                    "SHA-256": uuid.uuid4().hex,
                    "MD5": uuid.uuid4().hex[:32],
                    "SHA-1": uuid.uuid4().hex[:40]
                }
            } for _ in range(5)
        ]

        self.stdout.write(self.style.SUCCESS(f"Sending {n} messages to topic '{topic}'..."))

        for i in range(n):
            base = random.choice(base_messages)
            message = {
                "queue_name": "Conversion-Events_b2f4ff80",
                "actor_name": "ingest_conversion_events",
                "args": [
                    {
                        "filename": "directory.json",
                        "imported_timestamp": time.time(),
                        "source_id": base["source_id"],
                        "collection_id": base["collection_id"],
                        "data": {
                            "id": base["data_id"],
                            "objects": [
                                {
                                    "id": base["object_id"],
                                    "hashes": base["hashes"],
                                    "name": "query.dll"
                                }
                            ]
                        }
                    }
                ],
                "message_id": str(uuid.uuid4()),
                "message_timestamp": int(time.time() * 1000)
            }
            producer.send(topic, value=message)

            # Optional: flush every 500 messages
            if (i + 1) % 500 == 0:
                producer.flush()
                self.stdout.write(self.style.WARNING(f"Sent {i+1}/{n} messages..."))

        # Final flush
        producer.flush()
        self.stdout.write(self.style.SUCCESS(f"All {n} messages sent to topic '{topic}'!"))
