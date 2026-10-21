"""Estimate retained messages on source Kafka without consuming.

For each partition: latest_offset - earliest_offset. Committed offsets are
not used. Offset gaps (for example from compaction) can make the estimate
differ from the actual number of messages.
"""

import argparse
import math
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition

from config.settings import load_settings


TOPIC_NAME = "product_view"


def count_messages(consumer: Consumer, topic: str, timeout: float) -> int:
    """Sum partition offset ranges; never join a group or fetch messages."""
    metadata = consumer.list_topics(topic=topic, timeout=timeout)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None:
        raise RuntimeError(f"Topic not found: {topic}")
    if topic_metadata.error is not None:
        raise KafkaException(topic_metadata.error)
    if not topic_metadata.partitions:
        raise RuntimeError(f"No partitions found for topic: {topic}")

    partitions = [
        TopicPartition(topic, partition_id)
        for partition_id in sorted(topic_metadata.partitions)
    ]
    total = 0
    print(f"\nSource topic: {topic}")
    print(f"{'PARTITION':>10} {'EARLIEST':>14} {'LATEST':>14} {'ESTIMATE':>14}")
    for partition in partitions:
        partition_metadata = topic_metadata.partitions[partition.partition]
        if partition_metadata.error is not None:
            raise KafkaException(partition_metadata.error)
        watermarks = consumer.get_watermark_offsets(
            partition, timeout=timeout, cached=False
        )
        if watermarks is None:
            raise TimeoutError(f"Cannot read offsets for partition {partition.partition}")
        earliest, latest = watermarks
        if earliest < 0 or latest < earliest:
            raise RuntimeError(f"Invalid offset range for partition {partition.partition}")
        estimate = latest - earliest
        total += estimate
        print(
            f"{partition.partition:>10} {earliest:>14} "
            f"{latest:>14} {estimate:>14}"
        )

    print(f"Topic estimate: {total:,}")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config", type=Path,
        default=Path(__file__).resolve().parent / "config" / "config.yml",
    )
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--topic", default=TOPIC_NAME)
    scope.add_argument("--all-topics", action="store_true", help="Count all non-internal source topics")
    parser.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    parser.add_argument(
        "--rate", type=float,
        help="Measured aggregate mirror throughput in messages/second (all workers)",
    )
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("--timeout must be finite and greater than zero")
    if args.rate is not None and (not math.isfinite(args.rate) or args.rate <= 0):
        parser.error("--rate must be finite and greater than zero")

    settings = load_settings(args.config)
    consumer_config = settings.source.to_client_config()
    consumer_config.update({
        "group.id": "count_message_metadata",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
    })
    consumer = Consumer(consumer_config)
    try:
        if args.all_topics:
            metadata = consumer.list_topics(timeout=args.timeout)
            topics = sorted(name for name in metadata.topics if not name.startswith("__"))
        else:
            topics = [args.topic]
        total = sum(count_messages(consumer, topic, args.timeout) for topic in topics)
        print(f"\nTotal estimated messages across {len(topics)} topic(s): {total:,}")
        print("Formula: sum(latest_offset - earliest_offset) across partitions.")
        print("Offset gaps and changes during the query can affect the estimate.")
        if args.rate is not None:
            seconds = total / args.rate
            print(f"Mirror throughput: {args.rate:,.2f} messages/second")
            print(f"Estimated full mirror time: {seconds:,.1f} seconds ({seconds / 3600:,.2f} hours)")
            print("Assumes mirroring all retained data at a constant rate, with no new messages.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
