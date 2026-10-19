import unittest
import threading
from types import SimpleNamespace
from unittest.mock import patch
from dataclasses import replace

from mirror.group import Quota, transactional_id, Supervisor, Member, identity
from mirror.models import MirrorState
from mirror.worker import OutcomeUnknown
from config.mirror_settings import MirrorSettings
from config.settings import KafkaSettings
from tests.test_mirror_state import topology
from mirror.state import manifest_for
from tests.test_mirror_worker import Record


class MirrorGroupTests(unittest.TestCase):
    def test_partition_identity_survives_consumer_change(self):
        self.assertEqual(transactional_id('flow', 0), transactional_id('flow', 0))
        self.assertNotEqual(transactional_id('flow', 0), transactional_id('flow', 1))

    def test_parallel_reservations_cannot_exceed_global_commit_limit(self):
        quota = Quota(100)
        def worker():
            while quota.reserve(1): quota.complete(1)
        threads = [threading.Thread(target=worker) for _ in range(3)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        self.assertEqual(quota.committed, 100)
        self.assertFalse(quota.reserve(1))
        quota = Quota(1)
        self.assertTrue(quota.reserve(1))
        quota.release(1)
        self.assertTrue(quota.reserve(1))

    def test_startup_resolves_partition_transactions_before_reading_checkpoint(self):
        top = topology()
        client = KafkaSettings('localhost:1', 'PLAINTEXT', '', '', '', '', 'events')
        settings = MirrorSettings(client, client)
        owner = Supervisor(settings)
        flow = identity(top)
        initialized = set()
        def make_producer(settings, txid):
            return SimpleNamespace(init_transactions=lambda timeout: initialized.add(txid))
        def restore(*args):
            required = {transactional_id(flow, p) for p in [0, 1]}
            self.assertTrue(required <= initialized, 'Restore blocks on abandoned transactions unless all partition epochs are fenced first')
            return MirrorState(manifest_for(top), {0: 100, 1: 20})
        with patch('mirror.group.AdminClient'), patch('mirror.group.inspect_topology', return_value=top), \
             patch('mirror.group.ready_topology', return_value=top), \
             patch('mirror.group.ensure_destination'), patch('mirror.group.producer', side_effect=make_producer), \
             patch('mirror.group.read_state', side_effect=restore), \
             patch('mirror.group.get_offsets', return_value=top['bounds']), \
             patch('mirror.group.Member.run', side_effect=owner.stop.set):
            owner.run()

    def test_unknown_commit_keeps_quota_reserved_and_stops_other_threads(self):
        client = KafkaSettings('localhost:1', 'PLAINTEXT', '', '', '', '', 'events')
        owner = Supervisor(MirrorSettings(client, client, max_records=1))
        owner.topology = {'source_cluster_id': 'source'}
        member = Member(0, owner)
        member.sessions[0] = (object(), MirrorState({}, {0: 100}))
        with patch('mirror.group.commit_batch', side_effect=OutcomeUnknown('reply lost')):
            with self.assertRaises(OutcomeUnknown): member.process([Record()])
        self.assertTrue(owner.stop.is_set())
        self.assertFalse(owner.quota.reserve(1))
