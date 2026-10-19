"""Verify only the bounded canary sample against its exact source offsets."""
import argparse
from collections import Counter, defaultdict
import json
import time
from uuid import uuid4

from confluent_kafka import Consumer, TopicPartition, KafkaError, KafkaException
from config.mirror_settings import load_mirror_settings


def reader(client):
    return Consumer({**client.to_client_config(), 'group.id': 'mirror-sample-verify-' + uuid4().hex,
        'enable.auto.commit': False, 'enable.auto.offset.store': False, 'allow.auto.create.topics': False,
        'isolation.level': 'read_committed', 'auto.offset.reset': 'error', 'enable.partition.eof': True,
        'fetch.max.bytes': 1048576, 'max.partition.fetch.bytes': 1048576, 'log_level': 0})


def verify_sample(settings, expected_count):
    from confluent_kafka.admin import AdminClient
    from mirror.preflight import inspect_topology
    from mirror.state import read_state
    src = AdminClient({**settings.source.to_client_config(), 'log_level': 0})
    dst = AdminClient({**settings.destination.to_client_config(), 'log_level': 0})
    topology = inspect_topology(src, dst, settings)
    state = read_state(settings, topology)
    if state is None: raise ValueError('Canary state does not exist.')
    captured, by_partition, origins = {}, defaultdict(list), []
    target = reader(settings.destination)
    try:
        for partition in topology['partitions']:
            low, high = target.get_watermark_offsets(TopicPartition(settings.destination.topic, partition), timeout=15)
            target.assign([TopicPartition(settings.destination.topic, partition, low)])
            deadline = time.monotonic() + 30
            position = low
            while position < high:
                if time.monotonic() > deadline: raise TimeoutError('Canary read timed out.')
                message = target.poll(.2)
                if message is None: continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        position = message.offset()
                        continue
                    raise KafkaException(message.error())
                position = message.offset() + 1
                headers = message.headers() or []
                entries = [value for key, value in headers if key == '__mirror_origin_v1']
                if len(entries) != 1: raise ValueError('Missing or duplicate origin header.')
                origin = json.loads(entries[0])
                key = (origin['partition'], origin['offset'])
                if origin['cluster_id'] != topology['source_cluster_id'] or origin['topic'] != settings.source.topic:
                    raise ValueError('Canary origin identity mismatch.')
                if message.partition() != origin['partition']: raise ValueError('Partition mapping changed.')
                origins.append(key)
                by_partition[key[0]].append(key[1])
                captured[key] = (message.key(), message.value(), headers[:-1], message.timestamp()[1])
                if len(origins) > expected_count: raise ValueError('Canary exceeds the expected bounded sample.')
    finally:
        target.close()
    if len(origins) != expected_count or any(n != 1 for n in Counter(origins).values()):
        raise ValueError('Sample count or unique-origin count differs from expectation.')
    source = reader(settings.source)
    verified = 0
    try:
        for partition, offsets in by_partition.items():
            if offsets != sorted(offsets): raise ValueError('Source partition order changed.')
            if state.next_offsets[partition] != max(offsets) + 1: raise ValueError('Checkpoint does not match last canary record.')
            wanted = set(offsets)
            source.assign([TopicPartition(settings.source.topic, partition, min(offsets))])
            deadline = time.monotonic() + 30
            while wanted:
                if time.monotonic() > deadline: raise TimeoutError('Bounded source comparison timed out.')
                message = source.poll(.2)
                if message is None: continue
                if message.error(): raise KafkaException(message.error())
                if message.offset() > max(offsets): raise ValueError('Source record no longer readable.')
                if message.offset() not in wanted: continue
                actual = (message.key(), message.value(), message.headers() or [], message.timestamp()[1])
                if actual != captured[(partition, message.offset())]: raise ValueError('Source/canary bytes, headers or timestamp differ.')
                wanted.remove(message.offset())
                verified += 1
    finally:
        source.close()
    print(json.dumps({'canary_records': len(origins), 'unique_origins': len(set(origins)),
                      'source_records_compared': verified, 'payload_headers_timestamp': 'match',
                      'checkpoint': state.next_offsets, 'status': 'verified'}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config')
    parser.add_argument('--expected-records', type=int, default=100)
    args = parser.parse_args()
    if not 0 < args.expected_records <= 1000: parser.error('Sample size must be between 1 and 1000.')
    verify_sample(load_mirror_settings(args.config).canary(), args.expected_records)
