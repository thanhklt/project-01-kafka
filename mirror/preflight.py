"""Read-only topology inspection, with explicit destination-only provisioning."""
import time
from confluent_kafka import KafkaException, KafkaError, TopicCollection
from confluent_kafka.admin import ConfigResource, NewPartitions, NewTopic, ResourceType
from count_messages import get_offsets

DATA_CONFIG = {'cleanup.policy': 'delete', 'retention.ms': '-1', 'retention.bytes': '-1',
               'min.insync.replicas': '2', 'message.timestamp.type': 'CreateTime'}
STATE_CONFIG = {'cleanup.policy': 'compact', 'retention.ms': '-1', 'retention.bytes': '-1',
                'min.insync.replicas': '2', 'segment.bytes': '1048576', 'min.cleanable.dirty.ratio': '0.1'}


def topic_description(admin, topic, timeout, optional=False):
    try:
        return admin.describe_topics(TopicCollection([topic]), request_timeout=timeout)[topic].result(timeout)
    except KafkaException as exc:
        if optional and exc.args[0].code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            return None
        raise


def validate_topology(topology):
    if not topology.get('source_cluster_id') or not topology.get('destination_cluster_id'):
        raise ValueError('Cluster identity missing.')
    if topology['source_cluster_id'] == topology['destination_cluster_id']:
        distinct_local = (topology.get('allow_shared_cluster_id') is True
            and topology.get('expected_source_topic_id') == topology.get('source_topic_id')
            and topology.get('expected_source_topic_id')
            and topology.get('destination_source_topic_id') != topology.get('source_topic_id')
            and topology.get('source_brokers_remote') and topology.get('destination_brokers_local')
            and topology.get('source_broker_ids') != topology.get('destination_broker_ids'))
        if not distinct_local:
            raise ValueError('Source and destination share a cluster ID; distinct pinned local-cluster proof is required.')
    if topology.get('expected_source_topic_id') and topology['source_topic_id'] != topology['expected_source_topic_id']:
        raise ValueError('Source topic ID changed; refusing to mirror a recreated/different topic.')
    if not topology.get('source_topic_id') or not topology.get('partitions'):
        raise ValueError('Source topic ID/partitions missing.')


def topic_id(description):
    value = str(getattr(description, 'topic_id', '') or '')
    if value in ('', 'AAAAAAAAAAAAAAAAAAAAAA', '00000000-0000-0000-0000-000000000000'):
        raise ValueError('Broker must provide stable topic IDs.')
    return value


def inspect_topology(source, destination, settings):
    timeout = settings.operation_timeout_seconds
    src = topic_description(source, settings.source.topic, timeout)
    dst = topic_description(destination, settings.destination.topic, timeout, optional=True)
    cp = topic_description(destination, settings.checkpoint_topic, timeout, optional=True)
    topology = dict(source_cluster_id=source.describe_cluster(request_timeout=timeout).result(timeout).cluster_id,
                    destination_cluster_id=destination.describe_cluster(request_timeout=timeout).result(timeout).cluster_id,
                    source_topic=settings.source.topic, source_topic_id=topic_id(src),
                    destination_topic=settings.destination.topic, destination_topic_id=topic_id(dst) if dst else None,
                    checkpoint_topic_id=topic_id(cp) if cp else None,
                    partitions=sorted(p.id for p in src.partitions),
                    bounds=get_offsets(source, settings.source.topic, timeout))
    topology['allow_shared_cluster_id'] = settings.allow_shared_cluster_id
    topology['expected_source_topic_id'] = settings.expected_source_topic_id
    if topology['source_cluster_id'] == topology['destination_cluster_id']:
        src_metadata, dst_metadata = source.list_topics(timeout=timeout), destination.list_topics(timeout=timeout)
        loopback = {'localhost', '127.0.0.1', '::1'}
        topology.update(source_brokers_remote=all(b.host.lower() not in loopback for b in src_metadata.brokers.values()),
                        destination_brokers_local=all(b.host.lower() in loopback for b in dst_metadata.brokers.values()),
                        source_broker_ids=sorted(src_metadata.brokers), destination_broker_ids=sorted(dst_metadata.brokers))
        same_name = dst if settings.source.topic == settings.destination.topic else topic_description(destination, settings.source.topic, timeout, optional=True)
        topology['destination_source_topic_id'] = topic_id(same_name) if same_name else None
    validate_topology(topology)
    for name, desc, required in ((settings.destination.topic, dst, DATA_CONFIG), (settings.checkpoint_topic, cp, STATE_CONFIG)):
        if desc is None:
            continue
        resource = ConfigResource(ResourceType.TOPIC, name)
        actual = destination.describe_configs([resource], request_timeout=timeout)[resource].result(timeout)
        for key, value in required.items():
            if key in ('segment.bytes', 'min.cleanable.dirty.ratio'):
                continue
            if actual[key].value != value:
                raise ValueError(f'Destination topic {name}: incompatible {key}.')
        if any(len(p.replicas) != 3 for p in desc.partitions):
            raise ValueError(f'Destination topic {name} must have replication factor 3.')
        count = len(desc.partitions)
        if (name == settings.checkpoint_topic and count != 1) or (name == settings.destination.topic and count > len(src.partitions)):
            raise ValueError(f'Destination topic {name}: incompatible partition count.')
    return topology


def ensure_destination(destination, topology, settings):
    timeout = settings.operation_timeout_seconds
    for name, count, configs in ((settings.destination.topic, len(topology['partitions']), DATA_CONFIG),
                                  (settings.checkpoint_topic, 1, STATE_CONFIG)):
        desc = topic_description(destination, name, timeout, optional=True)
        if desc is None:
            destination.create_topics([NewTopic(name, count, 3, config=configs)], request_timeout=timeout)[name].result(timeout)
        elif len(desc.partitions) < count:
            destination.create_partitions([NewPartitions(name, count)], request_timeout=timeout)[name].result(timeout)


def ready_topology(source, destination, settings):
    deadline = time.monotonic() + settings.operation_timeout_seconds
    while True:
        try:
            topology = inspect_topology(source, destination, settings)
            if topology['destination_topic_id'] and topology['checkpoint_topic_id']:
                return topology
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                raise
        if time.monotonic() >= deadline:
            raise TimeoutError('Destination topic metadata has not propagated; no manifest was initialized.')
        time.sleep(.2)
