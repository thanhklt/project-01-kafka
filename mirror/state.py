"""Checkpoint state is authoritative only after a destination transaction commits."""
import json
import time
from uuid import uuid4
from confluent_kafka import Consumer, TopicPartition, KafkaException, KafkaError
from mirror.models import MirrorState

IDENTITY_FIELDS = ('source_cluster_id', 'source_topic_id', 'source_topic', 'destination_cluster_id',
                   'destination_topic_id', 'destination_topic', 'checkpoint_topic_id')


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode('utf8')


def manifest_for(topology):
    if any(not topology.get(key) for key in IDENTITY_FIELDS):
        raise ValueError('Cannot initialize manifest until every cluster/topic identity is known.')
    return {**{key: topology[key] for key in IDENTITY_FIELDS}, 'version': 1,
            'partitions': list(topology['partitions'])}


def decode_state(records):
    manifest, checkpoints = None, {}
    for key, raw in records:
        if key == b'control-heartbeat':
            continue
        try:
            value = json.loads(raw)
            if key == b'manifest':
                if not isinstance(value, dict): raise ValueError()
                manifest = value
            elif key and key.startswith(b'p:'):
                partition = int(key[2:])
                offset = value['next_offset']
                if value['version'] != 1 or type(offset) is not int or offset < 0: raise ValueError()
                checkpoints[partition] = offset
            else:
                raise ValueError()
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise ValueError('Corrupt or unsupported checkpoint record.') from None
    if manifest is None:
        if checkpoints: raise ValueError('Checkpoints present without manifest.')
        return None
    return MirrorState(manifest, checkpoints)


def restore_state(consumer, checkpoint_topic, topology, timeout):
    deadline = time.monotonic() + timeout
    low, target = consumer.get_watermark_offsets(TopicPartition(checkpoint_topic, 0), timeout=timeout, cached=False)
    consumer.assign([TopicPartition(checkpoint_topic, 0, low)])
    records = []
    while low < target:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('Checkpoint restore incomplete; refusing to assume first run.')
        message = consumer.poll(min(.2, remaining))
        if message is None: continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                low = message.offset()
                continue
            raise KafkaException(message.error())
        records.append((message.key(), message.value()))
        low = message.offset() + 1
    return decode_state(records)


def state_consumer(settings):
    return Consumer({**settings.destination.to_client_config(), 'group.id': 'mirror-state-' + uuid4().hex,
                     'enable.auto.commit': False, 'enable.auto.offset.store': False,
                     'auto.offset.reset': 'error', 'allow.auto.create.topics': False,
                     'isolation.level': 'read_committed', 'enable.partition.eof': True, 'log_level': 0})


def read_state(settings, topology):
    consumer = state_consumer(settings)
    try:
        return restore_state(consumer, settings.checkpoint_topic, topology, settings.operation_timeout_seconds)
    finally:
        consumer.close()


def validate_resume(state, topology):
    manifest = state.manifest
    if manifest.get('version') != 1 or any(manifest.get(key) != topology.get(key) for key in IDENTITY_FIELDS):
        raise ValueError('Mirror state identity/schema mismatch; refusing reset.')
    partitions = manifest.get('partitions')
    if not isinstance(partitions, list) or not partitions or any(type(p) is not int for p in partitions):
        raise ValueError('Invalid partition registry.')
    if set(state.next_offsets) != set(partitions):
        raise ValueError('Missing or unexpected partition checkpoint.')
    for partition in partitions:
        if partition not in topology['bounds']:
            raise ValueError('Registered source partition disappeared.')
        low, high = topology['bounds'][partition]
        offset = state.next_offsets[partition]
        if not low <= offset <= high:
            raise ValueError(f'Partition {partition}: checkpoint {offset} outside retained offsets [{low}, {high}].')
