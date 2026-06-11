import csv
import json
import logging
import signal
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
log = logging.getLogger(config.CONSUMER_ID)

_running = True


def _handle_signal(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

CSV_COLUMNS = [
    "consumer_id",
    "message_id",
    "comment_created_utc",
    "send_timestamp",
    "receive_timestamp",
    "finish_timestamp",
    "payload_bytes",
]


def main() -> None:
    log_subdir = Path(config.LOG_DIR) / config.EXP_LABEL
    log_subdir.mkdir(parents=True, exist_ok=True)
    csv_path = log_subdir / f"{config.CONSUMER_ID}.csv"

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": config.GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    # Track which partitions have received EOFs from which producers.
    # Key: partition id  Value: set of producer_id strings that sent EOF on that partition.
    # A consumer exits once every assigned partition has seen all NUM_PRODUCERS EOFs.
    assigned_partitions: set[int] = set()
    partition_eofs: dict[int, set[str]] = {}
    _initial_assignment_done = False

    def on_assign(_consumer, partitions):
        nonlocal _initial_assignment_done
        _initial_assignment_done = True
        for p in partitions:
            assigned_partitions.add(p.partition)
            if p.partition not in partition_eofs:
                partition_eofs[p.partition] = set()
        log.info("Assigned partitions: %s", sorted(assigned_partitions))

    def on_revoke(_consumer, partitions):
        for p in partitions:
            assigned_partitions.discard(p.partition)

    consumer.subscribe([config.TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    log.info(
        "Consumer %s starting — topic=%s group=%s num_producers=%d",
        config.CONSUMER_ID,
        config.TOPIC,
        config.GROUP_ID,
        config.NUM_PRODUCERS,
    )

    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    csv_file = open(csv_path, "a", newline="")
    writer = csv.writer(csv_file)
    if write_header:
        writer.writerow(CSV_COLUMNS)
        csv_file.flush()

    processed = 0
    _last_msg_time: float = 0.0  # set on first message; used for idle-timeout detection

    try:
        while _running:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                # Case 1: no partitions assigned (more consumers than partitions).
                if _initial_assignment_done and not assigned_partitions:
                    log.info("No partitions assigned — exiting (idle consumer)")
                    break
                # Case 2: assigned partitions whose EOF was already consumed by another
                # consumer before a rebalance. Poll returns nothing indefinitely.
                # Exit after 15 s of silence once at least one message has been seen.
                if (
                    _initial_assignment_done
                    and assigned_partitions
                    and _last_msg_time > 0
                    and (time.time() - _last_msg_time) > 15.0
                ):
                    log.info(
                        "No messages for 15 s after rebalance — partition(s) already "
                        "fully consumed by a previous consumer, exiting"
                    )
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            receive_timestamp = time.time()
            _last_msg_time = receive_timestamp

            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("Failed to decode message: %s", exc)
                consumer.commit(message=msg)
                continue

            if data.get("type") == "EOF":
                part = msg.partition()
                if part not in partition_eofs:
                    partition_eofs[part] = set()
                partition_eofs[part].add(data["producer_id"])
                consumer.commit(message=msg)
                log.debug(
                    "EOF from %s on partition %d (%d/%d for this partition)",
                    data["producer_id"],
                    part,
                    len(partition_eofs[part]),
                    config.NUM_PRODUCERS,
                )

                # Exit when every assigned partition has received EOFs from all producers
                if assigned_partitions and all(
                    len(partition_eofs.get(p, set())) >= config.NUM_PRODUCERS
                    for p in assigned_partitions
                ):
                    log.info("All partitions fully EOF'd, shutting down")
                    break
                continue

            # Simulate 1-second processing
            time.sleep(1.0)
            finish_timestamp = time.time()

            writer.writerow(
                [
                    config.CONSUMER_ID,
                    data.get("id", ""),
                    data.get("created_utc", ""),
                    data.get("send_timestamp", ""),
                    receive_timestamp,
                    finish_timestamp,
                    data.get("payload_bytes", 0),
                ]
            )
            csv_file.flush()
            consumer.commit(message=msg)
            processed += 1

            if processed % 20 == 0:
                log.info("Processed %d messages", processed)

    finally:
        consumer.close()
        csv_file.close()
        log.info("Consumer %s done — processed %d messages", config.CONSUMER_ID, processed)


if __name__ == "__main__":
    main()
