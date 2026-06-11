import json
import logging
import signal
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

import config
from dataset import generate_dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer-%(process)d] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

_running = True


def _handle_signal(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _create_topic() -> None:
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})
    new_topic = NewTopic(
        config.TOPIC,
        num_partitions=config.NUM_PARTITIONS,
        replication_factor=1,
    )
    futures = admin.create_topics([new_topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info("Created topic %s with %d partitions", topic_name, config.NUM_PARTITIONS)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("Topic %s already exists", topic_name)
            else:
                raise


def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed key=%s: %s", msg.key(), err)


def main() -> None:
    log.info(
        "Producer %d starting — messages=%d producers=%d partitions=%d topic=%s",
        config.PRODUCER_ID,
        config.NUM_MESSAGES,
        config.NUM_PRODUCERS,
        config.NUM_PARTITIONS,
        config.TOPIC,
    )

    _create_topic()

    producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 5,
            "batch.size": 65536,
        }
    )

    sent = 0
    total_bytes = 0

    for comment in generate_dataset(
        config.NUM_MESSAGES,
        config.NUM_PRODUCERS,
        config.PRODUCER_ID,
        config.FAKER_SEED,
    ):
        if not _running:
            break

        comment["send_timestamp"] = time.time()
        # First pass to measure size; second pass with size embedded
        raw = json.dumps(comment).encode("utf-8")
        comment["payload_bytes"] = len(raw)
        final_bytes = json.dumps(comment).encode("utf-8")

        producer.produce(
            config.TOPIC,
            value=final_bytes,
            key=str(comment["message_index"]).encode(),
            on_delivery=_delivery_report,
        )
        producer.poll(0)
        sent += 1
        total_bytes += len(final_bytes)

    producer.flush()
    log.info(
        "Producer %d: sent %d messages, %.2f KB total",
        config.PRODUCER_ID,
        sent,
        total_bytes / 1024,
    )

    # Send one EOF sentinel per partition so every consumer partition sees a termination signal.
    # This guarantees consumers can exit cleanly regardless of partition assignment.
    eof_value = json.dumps(
        {"type": "EOF", "producer_id": f"producer-{config.PRODUCER_ID}"}
    ).encode("utf-8")

    for partition in range(config.NUM_PARTITIONS):
        producer.produce(
            config.TOPIC,
            value=eof_value,
            partition=partition,
            on_delivery=_delivery_report,
        )
    producer.flush()
    log.info(
        "Producer %d: sent EOF sentinels on %d partitions",
        config.PRODUCER_ID,
        config.NUM_PARTITIONS,
    )


if __name__ == "__main__":
    main()
