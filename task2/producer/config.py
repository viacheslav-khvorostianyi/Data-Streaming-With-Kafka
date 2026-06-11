import os

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.environ.get("TOPIC", "reddit-comments")
NUM_MESSAGES = int(os.environ.get("NUM_MESSAGES", "200"))
NUM_PRODUCERS = int(os.environ.get("NUM_PRODUCERS", "1"))
NUM_PARTITIONS = int(os.environ.get("NUM_PARTITIONS", "1"))
FAKER_SEED = int(os.environ.get("FAKER_SEED", "42"))
EXP_LABEL = os.environ.get("EXP_LABEL", "smoke")

# PRODUCER_ID must be set explicitly via env var (docker compose scale gives all
# replicas the same hostname, so we cannot derive a unique ID from hostname).
PRODUCER_ID = int(os.environ.get("PRODUCER_ID", "0"))
