import csv
import json
import logging
import signal
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [generator] %(levelname)s %(message)s",
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
    futures = admin.create_topics([NewTopic(config.TOPIC, num_partitions=1, replication_factor=1)])
    for topic_name, future in futures.items():
        try:
            future.result()
            log.info("Created topic %s", topic_name)
        except KafkaException as exc:
            if exc.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                log.info("Topic %s already exists", topic_name)
            else:
                raise


def _delivery_report(err, msg):
    if err:
        log.error("Delivery failed key=%s: %s", msg.key(), err)


def main() -> None:
    log.info("Generator starting — file=%s topic=%s", config.HISTORY_FILE, config.TOPIC)
    _create_topic()

    producer = Producer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "batch.size": 1_048_576,        # 1 MB batches
        "compression.type": "lz4",
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 1_048_576,  # 1 GB in-flight buffer
    })

    poll_every = 1_000    # drain delivery callbacks without blocking every message
    log_every  = 100_000  # progress log interval

    sent = 0
    with open(config.HISTORY_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if not _running:
                break
            payload = json.dumps({
                "url": row["url"],
                "title": row.get("title", ""),
                "visit_time": row.get("visit_time", ""),
            }).encode("utf-8")
            producer.produce(
                config.TOPIC,
                value=payload,
                key=str(idx).encode(),
                on_delivery=_delivery_report,
            )
            sent += 1
            if sent % poll_every == 0:
                producer.poll(0)
            if sent % log_every == 0:
                log.info("Sent %d records so far…", sent)

    producer.flush()
    log.info("Sent %d URL records total", sent)

    eof = json.dumps({"type": "EOF"}).encode("utf-8")
    producer.produce(config.TOPIC, value=eof, partition=0, on_delivery=_delivery_report)
    producer.flush()
    log.info("Sent EOF sentinel")


if __name__ == "__main__":
    main()
