import os

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.environ.get("TOPIC", "browser.history.visits")
HISTORY_FILE = os.environ.get("HISTORY_FILE", "/data/history.csv")
