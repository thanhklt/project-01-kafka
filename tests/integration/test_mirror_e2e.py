"""Opt-in: a bounded local sample on two real clusters; never copies production history."""
import collections
from dataclasses import replace
import json
import os
from pathlib import Path
import threading
import time
import unittest
from uuid import uuid4

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KafkaSettings
from config.mirror_settings import load_mirror_settings
from mirror.group import Supervisor, producer, transactional_id, identity
from mirror.preflight import inspect_topology
from mirror.state import read_state, encode


@unittest.skipUnless(os.environ.get('MIRROR_INTEGRATION') == '1', 'Set MIRROR_INTEGRATION=1 for the bounded two-cluster sample')
class MirrorIntegrationTests(unittest.TestCase):
    def test_sample_restart_atomic_recovery_and_live_following(self):
        settings = load_mirror_settings()
        topic = 'mirror-it-' + uuid4().hex[:12]
        source = KafkaSettings('127.0.0.1:19094', 'PLAINTEXT', '', '', '', '', topic)
        settings = replace(settings, source=source, destination=replace(settings.destination, topic=topic),
                           checkpoint_topic=topic + '-state', metadata_refresh_seconds=2,
                           progress_interval_seconds=5, max_records=None,
                           allow_shared_cluster_id=False, expected_source_topic_id=None)
        admin = AdminClient({**source.to_client_config(), 'log_level': 0})
        admin.create_topics([NewTopic(topic, 3, 1)], request_timeout=30)[topic].result(30)
        expected = {}
        upstream = Producer({**source.to_client_config(), 'log_level': 0})

        def add_records(start, count):
            failures = []
            for n in range(start, start + count):
                value = None if n % 7 == 0 else b'\xffsample' if n % 5 == 0 else b'repeated-value'
                def delivered(error, message, value=value):
                    if error: failures.append(error)
                    else: expected[(message.partition(), message.offset())] = (message.key(), value, message.headers())
                upstream.produce(topic, partition=n % 3, key=b'key', value=value,
                                 headers=[('duplicate', b'a'), ('duplicate', None)], on_delivery=delivered)
            self.assertEqual(upstream.flush(30), 0)
            self.assertEqual(failures, [])

        def start_worker():
            worker = Supervisor(settings)
            failures = []
            def run():
                try: worker.run()
                except Exception as exc: failures.append(exc)
            thread = threading.Thread(target=run, daemon=True)
            thread.start()
            return worker, thread, failures

        def await_count(worker, failures, count):
            deadline = time.monotonic() + 90
            while worker.quota.committed < count:
                if failures: raise failures[0]
                if time.monotonic() >= deadline: self.fail('Sample did not catch up within 90 seconds')
                time.sleep(.1)

        def stop_worker(worker, thread, failures):
            worker.stop.set()
            thread.join(60)
            self.assertFalse(thread.is_alive())
            if failures: raise failures[0]

        add_records(0, 30)
        worker, thread, failures = start_worker()
        try:
            await_count(worker, failures, 30)
            self.assertEqual(len([m for m in worker.members if m.sessions]), 3)
            add_records(30, 6)
            await_count(worker, failures, 36)
        finally:
            stop_worker(worker, thread, failures)

        # Stage a duplicate and a bogus checkpoint without committing. Reassignment init must abort both.
        src_admin = AdminClient({**source.to_client_config(), 'log_level': 0})
        dst_admin = AdminClient({**settings.destination.to_client_config(), 'log_level': 0})
        topology = inspect_topology(src_admin, dst_admin, settings)
        original = read_state(settings, topology)
        abandoned = producer(settings, transactional_id(identity(topology), 0))
        abandoned.init_transactions(30)
        abandoned.begin_transaction()
        abandoned.produce(settings.destination.topic, partition=0, value=b'UNCOMMITTED-DUPLICATE')
        abandoned.produce(settings.checkpoint_topic, partition=0, key=b'p:0', value=encode({'version': 1, 'next_offset': 99999}))
        self.assertEqual(abandoned.flush(30), 0)

        worker, thread, failures = start_worker()
        try:
            deadline = time.monotonic() + 90
            while len([m for m in worker.members if m.sessions]) != 3:
                if failures: raise failures[0]
                if time.monotonic() > deadline: self.fail('Restart assignment timed out')
                time.sleep(.1)
            self.assertEqual(worker.quota.committed, 0)
            add_records(36, 3)
            await_count(worker, failures, 3)
        finally:
            stop_worker(worker, thread, failures)

        received = []
        reader = Consumer({**settings.destination.to_client_config(), 'group.id': 'sample-verify-' + uuid4().hex,
                           'enable.auto.commit': False, 'isolation.level': 'read_committed', 'log_level': 0})
        try:
            reader.assign([TopicPartition(topic, p, 0) for p in range(3)])
            deadline = time.monotonic() + 30
            while len(received) < 39 and time.monotonic() < deadline:
                message = reader.poll(.2)
                if message is None: continue
                if message.error(): raise RuntimeError(str(message.error()))
                received.append(message)
        finally:
            reader.close()
        self.assertEqual(len(received), 39)
        origins = []
        per_partition = collections.defaultdict(list)
        for message in received:
            headers = message.headers()
            origin = json.loads(next(value for name, value in headers if name == '__mirror_origin_v1'))
            origin_key = (origin['partition'], origin['offset'])
            origins.append(origin_key)
            key, value, _ = expected[origin_key]
            self.assertEqual(message.key(), key)
            self.assertEqual(message.value(), value)
            self.assertEqual(headers[:-1], [('duplicate', b'a'), ('duplicate', None)])
            self.assertEqual(message.partition(), origin['partition'])
            per_partition[message.partition()].append(origin['offset'])
        self.assertEqual(set(origins), set(expected))
        self.assertTrue(all(count == 1 for count in collections.Counter(origins).values()))
        for offsets in per_partition.values(): self.assertEqual(offsets, sorted(offsets))
        final = read_state(settings, topology)
        self.assertEqual(final.next_offsets, {0: 13, 1: 13, 2: 13})
        print('SAMPLE VERIFIED: 39 records, 3 consumers, restart, uncommitted transaction recovery, no duplicate origin.')


if __name__ == '__main__':
    unittest.main()
