import unittest
from types import SimpleNamespace

from confluent_kafka import KafkaError, KafkaException

from mirror.models import MirrorState
from mirror.worker import prepare_batch, commit_batch, RetryBatch, OutcomeUnknown


class Record:
    def __init__(self, offset=100, partition=0, value=b'\xff', headers=None):
        self.o, self.p, self.v, self.h = offset, partition, value, headers
    def topic(self): return 'events'
    def error(self): return None
    def partition(self): return self.p
    def offset(self): return self.o
    def key(self): return None
    def value(self): return self.v
    def headers(self): return self.h or [('x', b'a'), ('x', None)]
    def timestamp(self): return (1, 123456789)


class TransactionBroker:
    """Boundary fake: staged records only become visible together on commit."""
    def __init__(self, failure=None):
        self.visible, self.staged = [], []
        self.failure = failure
        self.attempts = 0
    def begin_transaction(self): self.staged = []
    def produce(self, **kwargs): self.staged.append(kwargs)
    def poll(self, timeout): return 0
    def commit_transaction(self, timeout):
        self.attempts += 1
        if self.failure:
            failure, self.failure = self.failure, None
            raise failure
        self.visible.extend(self.staged)
        self.staged = []
    def abort_transaction(self, timeout): self.staged = []


def settings():
    return SimpleNamespace(destination=SimpleNamespace(topic='events'), checkpoint_topic='state',
                           operation_timeout_seconds=.02, retry_backoff_initial_ms=1, retry_backoff_max_ms=2)


class MirrorWorkerTests(unittest.TestCase):
    def test_preserves_record_and_distinct_offsets_with_same_payload(self):
        batch = prepare_batch([Record(100), Record(104, value=None)], 'source')
        self.assertEqual(batch.next_offsets, {0: 105})
        self.assertEqual(batch.records[0]['value'], b'\xff')
        self.assertIsNone(batch.records[1]['value'])
        self.assertEqual(batch.records[0]['headers'][:2], [('x', b'a'), ('x', None)])
        self.assertEqual(batch.records[0]['timestamp'], 123456789)
        with self.assertRaises(ValueError): prepare_batch([Record(), Record(partition=1)], 'source')
        with self.assertRaises(ValueError): prepare_batch([Record(headers=[('__mirror_origin_v1', b'x')])], 'source')

    def test_data_and_checkpoint_publish_atomically(self):
        broker = TransactionBroker()
        state = MirrorState({}, {0: 100})
        commit_batch(broker, prepare_batch([Record()], 'source'), state, settings())
        self.assertEqual(len(broker.visible), 2)
        self.assertEqual(broker.visible[1]['key'], b'p:0')
        self.assertEqual(state.next_offsets, {0: 101})

    def test_retry_commit_does_not_produce_batch_twice(self):
        broker = TransactionBroker(KafkaException(KafkaError(KafkaError._TIMED_OUT, retriable=True)))
        commit_batch(broker, prepare_batch([Record()], 'source'), MirrorState({}, {0: 100}), settings())
        self.assertEqual(len(broker.visible), 2)
        self.assertEqual(broker.attempts, 2)

    def test_abortable_transaction_never_advances_checkpoint(self):
        broker = TransactionBroker(KafkaException(KafkaError(KafkaError._STATE, txn_requires_abort=True)))
        state = MirrorState({}, {0: 100})
        with self.assertRaises(RetryBatch):
            commit_batch(broker, prepare_batch([Record()], 'source'), state, settings())
        self.assertEqual(state.next_offsets, {0: 100})
        self.assertEqual(broker.visible, [])

    def test_successful_commit_with_lost_reply_is_not_reproduced(self):
        class LostReplyBroker(TransactionBroker):
            def commit_transaction(self, timeout):
                super().commit_transaction(timeout)
                if self.attempts == 1:
                    raise KafkaException(KafkaError(KafkaError._TIMED_OUT, retriable=True))
        broker = LostReplyBroker()
        state = MirrorState({}, {0: 100})
        commit_batch(broker, prepare_batch([Record()], 'source'), state, settings())
        self.assertEqual(len(broker.visible), 2)
        self.assertEqual(state.next_offsets, {0: 101})
