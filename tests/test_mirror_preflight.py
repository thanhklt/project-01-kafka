import unittest
from unittest.mock import patch
from types import SimpleNamespace
from confluent_kafka import KafkaException, KafkaError
from mirror.preflight import validate_topology, DATA_CONFIG, STATE_CONFIG, ready_topology


class MirrorPreflightTests(unittest.TestCase):
    def test_provision_barrier_retries_only_transient_topic_propagation(self):
        ready = {'destination_topic_id': 'data', 'checkpoint_topic_id': 'state'}
        missing = KafkaException(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))
        with patch('mirror.preflight.inspect_topology', side_effect=[missing, ready]), patch('mirror.preflight.time.sleep'):
            self.assertEqual(ready_topology(None, None, SimpleNamespace(operation_timeout_seconds=1)), ready)

    def test_refuses_same_cluster_and_missing_topic_identity(self):
        base = dict(source_cluster_id='s', destination_cluster_id='d', source_topic_id='topic',
                    partitions=[0, 1, 2], bounds={0: (0, 0), 1: (0, 0), 2: (0, 0)})
        validate_topology(base)
        for change in ({'destination_cluster_id': 's'}, {'source_topic_id': None}, {'partitions': []}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_topology(base | change)

    def test_data_retains_events_state_compacts_without_expiring(self):
        self.assertEqual(DATA_CONFIG['cleanup.policy'], 'delete')
        self.assertEqual(STATE_CONFIG['cleanup.policy'], 'compact')
        self.assertEqual(STATE_CONFIG['retention.ms'], '-1')
        self.assertEqual(DATA_CONFIG['min.insync.replicas'], '2')

    def test_reused_cluster_id_requires_pinned_source_and_distinct_local_namespace(self):
        base = dict(source_cluster_id='same', destination_cluster_id='same', source_topic_id='source-topic',
                    partitions=[0], allow_shared_cluster_id=True, expected_source_topic_id='source-topic',
                    destination_source_topic_id=None, source_brokers_remote=True, destination_brokers_local=True,
                    source_broker_ids=[0], destination_broker_ids=[0, 1, 2])
        validate_topology(base)
        for change in ({'allow_shared_cluster_id': False}, {'expected_source_topic_id': 'other'},
                       {'destination_source_topic_id': 'source-topic'}, {'destination_brokers_local': False},
                       {'source_broker_ids': [0, 1, 2]}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_topology(base | change)
