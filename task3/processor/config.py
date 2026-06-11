import os

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.environ.get("TOPIC", "browser.history.visits")
GROUP_ID = os.environ.get("GROUP_ID", "tld-counter")
TOP_N = int(os.environ.get("TOP_N", "5"))
PRINT_INTERVAL = int(os.environ.get("PRINT_INTERVAL", "100000"))
