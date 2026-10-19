"""Read a small Kafka sample: poetry run python consume_sample.py --max-messages 10."""

import argparse
import base64
import json
import math
import sys
import time
from uuid import uuid4

from confluent_kafka import Consumer, KafkaException

from config.settings import load_settings


def decode(data: bytes | None):
    """Keep JSON structured, text readable, and non-UTF-8 bytes lossless."""
    if data is None:
        return None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return {"encoding": "base64", "data": base64.b64encode(data).decode("ascii")}
    try:
        return json.loads(text)
    except ValueError:
        return text


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="YAML config path (default: config/config.yml)")
    parser.add_argument("--topic", help="Override the topic in config")
    parser.add_argument("--from-beginning")
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--timeout", type=float, default=30, help="Total read timeout in seconds (default: 30)")
    args = parser.parse_args()
    if args.max_messages <= 0:
        parser.error("--max-messages must be positive")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")

    consumer = None
    count = 0
    try:
        settings = load_settings(args.config).kafka
        config = settings.to_client_config()
        config.update({
            "group.id": f"profile-sample-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        })
        consumer = Consumer(config)
        consumer.subscribe([args.topic or settings.topic])
        deadline = time.monotonic() + args.timeout
        while count < args.max_messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            message = consumer.poll(timeout=min(1.0, remaining))
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            record = {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
                "timestamp_ms": message.timestamp()[1],
                "key": decode(message.key()),
                "headers": [[key, decode(value)] for key, value in (message.headers() or [])],
                "value": decode(message.value()),
            }
            # ASCII escapes keep redirected output portable on Windows.
            print(json.dumps(record, ensure_ascii=True), flush=True)
            count += 1
        print(f"Read {count}/{args.max_messages} messages." + (" Timeout reached." if count < args.max_messages else ""), file=sys.stderr)
        return 0 if count == args.max_messages else 1
    except KeyboardInterrupt:
        print(f"Stopped after {count} messages.", file=sys.stderr)
        return 130
    except (OSError, ValueError, KafkaException) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        if consumer is not None:
            consumer.close()


if __name__ == "__main__":
    raise SystemExit(main())
