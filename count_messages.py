"""Estimate retained Kafka messages from partition offsets, without reading payloads."""

import argparse
import math
import sys
import time

from confluent_kafka import IsolationLevel, KafkaException, TopicCollection, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from config.settings import load_settings


def get_offsets(admin: AdminClient, topic: str, timeout: float) -> dict[int, tuple[int, int]]:
    """Return partition -> (earliest, latest), using one shared query deadline."""
    deadline = time.monotonic() + timeout

    def remaining() -> float:
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("Kafka query timeout")
        return seconds

    descriptions = admin.describe_topics(
        TopicCollection([topic]), request_timeout=remaining()
    )
    description = descriptions[topic].result(timeout=remaining())
    partitions = [TopicPartition(topic, partition.id) for partition in description.partitions]
    if not partitions:
        raise ValueError(f"Topic {topic!r} has no partitions")

    bounds = []
    for spec in (OffsetSpec.earliest(), OffsetSpec.latest()):
        futures = admin.list_offsets(
            {partition: spec for partition in partitions},
            isolation_level=IsolationLevel.READ_UNCOMMITTED,
            request_timeout=remaining(),
        )
        offsets = {}
        for partition in partitions:
            offsets[partition.partition] = futures[partition].result(timeout=remaining()).offset
        bounds.append(offsets)

    result = {}
    for partition in sorted(bounds[0]):
        earliest, latest = bounds[0][partition], bounds[1][partition]
        if earliest < 0 or latest < earliest:
            raise ValueError(
                f"Invalid offsets for partition {partition}: earliest={earliest}, latest={latest}. "
                "The log may have changed during the query; try again."
            )
        result[partition] = (earliest, latest)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", help="YAML config path (default: config/mirror.yml)")
    parser.add_argument("--timeout", type=float, default=30, help="Total query timeout in seconds (default: 30)")
    args = parser.parse_args(argv)
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and positive")

    settings = None
    try:
        settings = load_settings(args.config).kafka
        config = settings.to_client_config()
        # Report failures once through the CLI, instead of repeated background logs.
        config["log_level"] = 0
        admin = AdminClient(config)
        offsets = get_offsets(admin, settings.topic, args.timeout)

        lines = [f"Topic: {settings.topic}", "partition\tearliest\tlatest\testimated_messages"]
        total = 0
        for partition, (earliest, latest) in sorted(offsets.items()):
            estimate = latest - earliest
            total += estimate
            lines.append(f"{partition}\t{earliest}\t{latest}\t{estimate}")
        lines.append(f"Estimated total messages: {total}")
        lines.append(
            "Estimate only: offset gaps, compaction, transactions and concurrent writes/deletions "
            "can affect the result. Partitions are not measured as one atomic snapshot."
        )
        print("\n".join(lines))
        return 0
    except KeyboardInterrupt:
        print("Stopped by user.", file=sys.stderr)
        return 130
    except (KafkaException, OSError, ValueError) as exc:
        message = "Kafka query timeout; check broker connectivity." if isinstance(exc, TimeoutError) else str(exc)
        if settings is not None:
            for credential in sorted({settings.username, settings.password}, key=len, reverse=True):
                if credential:
                    message = message.replace(credential, "<redacted>")
        print(f"Error: {message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
