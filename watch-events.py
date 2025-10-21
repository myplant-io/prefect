#!/usr/bin/env python3
"""Subscribe to the Redis 'events' stream and print events matching a filter."""

import json
import sys

import redis

STREAM = "events"

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

last_id = "$"  # only new messages; use "0" to read from beginning

print(f"Listening on stream '{STREAM}'")
print("Press Ctrl+C to stop.\n")

def filter(event):
    return "prefect.deployment.6e795ba4-6dda-4f74-89be-b78ea8a7596f" in event

try:
    while True:
        entries = r.xread({STREAM: last_id}, block=1000, count=100)
        if not entries:
            continue

        for _stream_name, messages in entries:
            for msg_id, fields in messages:
                last_id = msg_id
                data = fields.get("data", "")
                if filter(data):
                    event = json.loads(data)
                    print(f"[{msg_id}] {event['event']}  id={event['id']}")
                    print(json.dumps(event, indent=2))
                    print()
except KeyboardInterrupt:
    print("\nStopped.")
    sys.exit(0)
