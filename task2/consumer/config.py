import os
import socket

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.environ.get("TOPIC", "reddit-comments")
GROUP_ID = os.environ.get("GROUP_ID", "reddit-consumers")
NUM_PRODUCERS = int(os.environ.get("NUM_PRODUCERS", "1"))
EXP_LABEL = os.environ.get("EXP_LABEL", "smoke")
LOG_DIR = os.environ.get("CONSUMER_LOG_DIR", "/logs")

CONSUMER_ID = socket.gethostname()
