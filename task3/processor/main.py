import json
import logging
import signal
from collections import Counter
from urllib.parse import urlparse

from confluent_kafka import Consumer, KafkaError

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [processor] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

_running = True


def _handle_signal(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def _extract_tld(url: str) -> str | None:
    # Take the final label of the hostname (e.g. "com", "ua", "org", "edu")
    try:
        host = urlparse(url).hostname or ""
        parts = [p for p in host.split(".") if p]
        return parts[-1].lower() if parts else None
    except Exception:
        return None


def _print_top(counts: Counter, top_n: int, label: str = "current") -> None:
    top = counts.most_common(top_n)
    if not top:
        return
    width = 37
    bar = "─" * width
    print(f"\n{bar}")
    print(f" TOP-{top_n} TLDs ({label})")
    print(f"{'─' * width}")
    print(f" {'RANK':>4} │ {'TLD':<12} │ {'VISITS':>7}")
    print(f"{'─' * width}")
    for rank, (tld, count) in enumerate(top, start=1):
        print(f" {rank:>4} │ {tld:<12} │ {count:>7}")
    print(f"{bar}\n")


def main() -> None:
    log.info(
        "Processor starting — topic=%s group=%s top_n=%d print_interval=%d",
        config.TOPIC, config.GROUP_ID, config.TOP_N, config.PRINT_INTERVAL,
    )

    consumer = Consumer({
        "bootstrap.servers": config.BOOTSTRAP_SERVERS,
        "group.id": config.GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([config.TOPIC])

    counts: Counter = Counter()
    processed = 0

    try:
        while _running:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Kafka error: %s", msg.error())
                continue

            try:
                data = json.loads(msg.value())
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("Failed to decode message: %s", exc)
                consumer.commit(message=msg)
                continue

            if data.get("type") == "EOF":
                log.info("Received EOF — printing final results")
                consumer.commit(message=msg)
                _print_top(counts, config.TOP_N, label="FINAL")
                break

            tld = _extract_tld(data.get("url", ""))
            if tld:
                counts[tld] += 1
            consumer.commit(message=msg)
            processed += 1

            if processed % config.PRINT_INTERVAL == 0:
                _print_top(counts, config.TOP_N, label=f"after {processed} messages")

    finally:
        consumer.close()
        log.info("Processor done — processed %d messages", processed)


if __name__ == "__main__":
    main()
