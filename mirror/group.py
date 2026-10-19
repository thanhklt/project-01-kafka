"""Three group members; producer epochs and checkpoints follow partitions, not threads."""
import hashlib
import json
import queue
import threading
import time
from collections import defaultdict

from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException, KafkaError
from confluent_kafka.admin import AdminClient
from count_messages import get_offsets
from mirror.models import MirrorState
from mirror.preflight import inspect_topology, ensure_destination, ready_topology
from mirror.state import read_state, validate_resume, manifest_for, encode
from mirror.worker import commit_batch, prepare_batch, commit_records, retry_operation, RetryBatch


def identity(topology):
    text = '\0'.join(str(topology[k]) for k in ('source_cluster_id', 'source_topic', 'destination_topic'))
    return hashlib.sha256(text.encode()).hexdigest()[:32]


def transactional_id(identity, partition):
    return f'mirror-v1-{identity}-p{partition}'


def producer(settings, txid):
    return Producer({**settings.destination.to_client_config(), 'transactional.id': txid,
                     'enable.idempotence': True, 'acks': 'all', 'max.in.flight.requests.per.connection': 5,
                     'transaction.timeout.ms': settings.transaction_timeout_ms, 'log_level': 0})


class Quota:
    def __init__(self, limit):
        self.limit, self.committed, self.inflight, self.bytes = limit, 0, 0, 0
        self.offsets = {}
        self.lock = threading.Lock()

    def reserve(self, count):
        with self.lock:
            if self.limit is not None and self.committed + self.inflight + count > self.limit: return False
            self.inflight += count
            return True

    def complete(self, count, size=0, offsets=None):
        with self.lock:
            self.inflight -= count
            self.committed += count
            self.bytes += size
            self.offsets.update(offsets or {})

    def release(self, count):
        with self.lock: self.inflight -= count

    def reached(self):
        with self.lock: return self.limit is not None and self.committed >= self.limit


class Member:
    def __init__(self, index, supervisor):
        self.index, self.owner, self.settings = index, supervisor, supervisor.settings
        self.consumer = None
        self.sessions = {}
        self.pending = set()
        self.admin = None

    def restore_partition(self, partition):
        tx = producer(self.settings, transactional_id(self.owner.flow, partition))
        retry_operation(lambda timeout: tx.init_transactions(timeout), self.settings)
        topology = dict(self.owner.topology)
        state = read_state(self.settings, topology)
        if state is None: raise ValueError('Partition assignment has no durable manifest.')
        # Read source bounds AFTER checkpoint, so concurrent writes cannot make the observed high stale.
        topology['bounds'] = get_offsets(self.admin, self.settings.source.topic, self.settings.operation_timeout_seconds)
        validate_resume(state, topology)
        if partition not in state.next_offsets: raise ValueError('Assigned partition has no checkpoint.')
        self.sessions[partition] = (tx, state)
        return state.next_offsets[partition]

    def on_assign(self, consumer, partitions):
        assigned = []
        paused = []
        for part in partitions:
            if part.partition not in self.owner.ready:
                self.pending.add(part.partition)
                paused.append(TopicPartition(part.topic, part.partition))
                assigned.append(TopicPartition(part.topic, part.partition, 0))
            else:
                offset = self.restore_partition(part.partition)
                assigned.append(TopicPartition(part.topic, part.partition, offset))
        consumer.assign(assigned)
        if paused: consumer.pause(paused)
        print(json.dumps({'member': self.index, 'assigned': [p.partition for p in partitions]}), flush=True)

    def on_revoke(self, consumer, partitions):
        # Callbacks run on this member's poll/consume thread, never concurrently with commit_batch.
        for part in partitions:
            self.sessions.pop(part.partition, None)
            self.pending.discard(part.partition)

    def on_lost(self, consumer, partitions):
        self.on_revoke(consumer, partitions)

    def process(self, messages):
        by_partition = defaultdict(list)
        for message in messages:
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF: continue
                raise KafkaException(message.error())
            if message.partition() not in self.sessions: continue
            tx, state = self.sessions[message.partition()]
            if message.offset() < state.next_offsets[message.partition()]: continue
            by_partition[message.partition()].append(message)
        for partition, records in by_partition.items():
            tx, state = self.sessions[partition]
            while records and not self.owner.stop.is_set():
                chunk, size = [], 0
                for record in records:
                    record_size = len(record.value() or b'') + len(record.key() or b'')
                    if chunk and size + record_size > self.settings.batch_max_bytes: break
                    if not self.owner.quota.reserve(1): break
                    chunk.append(record)
                    size += record_size
                if not chunk:
                    if self.owner.quota.reached(): self.owner.stop.set()
                    else: self.owner.stop.wait(.05)
                    continue
                try:
                    batch = prepare_batch(chunk, self.owner.topology['source_cluster_id'])
                    commit_batch(tx, batch, state, self.settings)
                except RetryBatch:
                    self.owner.quota.release(len(chunk))
                    self.consumer.seek(TopicPartition(self.settings.source.topic, partition, state.next_offsets[partition]))
                    break
                except Exception:
                    # A commit may have succeeded remotely. Keep the reservation until restart;
                    # releasing it would let another member exceed the global sample limit.
                    self.owner.stop.set()
                    raise
                self.owner.quota.complete(len(chunk), size, batch.next_offsets)
                del records[:len(chunk)]
                if self.owner.quota.reached(): self.owner.stop.set()

    def run(self):
        try:
            self.admin = AdminClient({**self.settings.source.to_client_config(), 'log_level': 0})
            self.consumer = Consumer({**self.settings.source.to_client_config(),
                'group.id': 'mirror-v1-' + self.owner.flow, 'client.id': f'mirror-member-{self.index}',
                'enable.auto.commit': False, 'enable.auto.offset.store': False, 'auto.offset.reset': 'error',
                'allow.auto.create.topics': False, 'isolation.level': 'read_committed',
                'partition.assignment.strategy': 'range', 'group.protocol': 'classic',
                'max.poll.interval.ms': 300000, 'session.timeout.ms': 45000,
                'fetch.max.bytes': self.settings.batch_max_bytes, 'log_level': 0})
            self.consumer.subscribe([self.settings.source.topic], on_assign=self.on_assign,
                                    on_revoke=self.on_revoke, on_lost=self.on_lost)
            while not self.owner.stop.is_set():
                for partition in list(self.pending & self.owner.ready):
                    offset = self.restore_partition(partition)
                    tp = TopicPartition(self.settings.source.topic, partition, offset)
                    self.consumer.seek(tp)
                    self.consumer.resume([tp])
                    self.pending.remove(partition)
                messages = self.consumer.consume(self.settings.batch_max_records, timeout=min(.5, self.settings.batch_wait_ms / 1000))
                self.process(messages)
        except Exception as exc:
            self.owner.errors.put(exc)
            self.owner.stop.set()
        finally:
            if self.consumer is not None:
                try: self.consumer.close()
                except Exception as exc: self.owner.errors.put(exc)
            self.sessions.clear()


class Supervisor:
    def __init__(self, settings):
        self.settings = settings
        self.stop = threading.Event()
        self.errors = queue.Queue()
        self.quota = Quota(settings.max_records)
        self.ready = set()
        self.topology = {}
        self.threads = []
        self.members = []

    def register(self, state, topology):
        new = set(topology['partitions']) - set(state.next_offsets if state else [])
        if state is None:
            # An empty retained range is insufficient: high > 0 proves this log was already written.
            bounds = get_offsets(self.dst, self.settings.destination.topic, self.settings.operation_timeout_seconds)
            if any(high != 0 for _, high in bounds.values()):
                raise ValueError('Destination was already written but has no matching mirror manifest.')
            state = MirrorState(manifest_for(topology), {})
        if new:
            manifest = dict(state.manifest, partitions=sorted(set(state.next_offsets) | new))
            records = [dict(topic=self.settings.checkpoint_topic, partition=0, key=b'manifest', value=encode(manifest))]
            offsets = {p: topology['bounds'][p][0] for p in new}
            records += [dict(topic=self.settings.checkpoint_topic, partition=0, key=f'p:{p}'.encode(),
                             value=encode({'version': 1, 'next_offset': offset})) for p, offset in offsets.items()]
            commit_records(self.control, records, self.settings)
            state = MirrorState(manifest, state.next_offsets | offsets)
        self.ready = set(state.next_offsets)
        return state

    def refresh(self):
        state = read_state(self.settings, self.topology)
        topology = inspect_topology(self.src, self.dst, self.settings)
        if state is None: raise ValueError('Manifest disappeared during mirror operation.')
        validate_resume(state, topology)
        ensure_destination(self.dst, topology, self.settings)
        self.register(state, topology)
        self.topology = topology
        commit_records(self.control, [dict(topic=self.settings.checkpoint_topic, partition=0,
                        key=b'control-heartbeat', value=encode({'at': time.time()}))], self.settings)

    def progress(self, started):
        with self.quota.lock:
            offsets = dict(self.quota.offsets)
            committed, size = self.quota.committed, self.quota.bytes
        gaps = {str(p): max(0, self.topology['bounds'][p][1] - offset) for p, offset in offsets.items()}
        print(json.dumps({'committed': committed, 'bytes': size, 'records_per_second': round(committed / max(.001, time.monotonic() - started), 2),
                          'next_offsets': offsets, 'observed_offset_gap': gaps,
                          'status': 'following' if len(offsets) == len(self.ready) and not any(gaps.values()) else 'catching_up'}), flush=True)

    def run(self):
        self.src = AdminClient({**self.settings.source.to_client_config(), 'log_level': 0})
        self.dst = AdminClient({**self.settings.destination.to_client_config(), 'log_level': 0})
        topology = inspect_topology(self.src, self.dst, self.settings)
        ensure_destination(self.dst, topology, self.settings)
        self.topology = ready_topology(self.src, self.dst, self.settings)
        self.flow = identity(self.topology)
        self.control = producer(self.settings, 'mirror-v1-' + self.flow + '-control')
        retry_operation(lambda timeout: self.control.init_transactions(timeout), self.settings)
        # Any abandoned partition checkpoint transaction can hold the state topic LSO below
        # its high watermark. Resolve every data producer epoch BEFORE awaiting that barrier.
        # Assignment will acquire a newer epoch again, immediately before its own restore.
        startup_epochs = []
        for partition in self.topology['partitions']:
            tx = producer(self.settings, transactional_id(self.flow, partition))
            retry_operation(lambda timeout: tx.init_transactions(timeout), self.settings)
            startup_epochs.append(tx)
        state = read_state(self.settings, self.topology)
        if state is not None:
            self.topology['bounds'] = get_offsets(self.src, self.settings.source.topic, self.settings.operation_timeout_seconds)
            validate_resume(state, self.topology)
        self.register(state, self.topology)
        self.members = [Member(index, self) for index in range(self.settings.consumer_count)]
        self.threads = [threading.Thread(target=member.run, name=f'mirror-consumer-{member.index}', daemon=True) for member in self.members]
        started = last_refresh = last_progress = time.monotonic()
        for thread in self.threads: thread.start()
        try:
            while not self.stop.wait(.2):
                now = time.monotonic()
                if now - last_refresh >= self.settings.metadata_refresh_seconds:
                    self.refresh()
                    last_refresh = time.monotonic()
                if now - last_progress >= self.settings.progress_interval_seconds:
                    self.progress(started)
                    last_progress = time.monotonic()
        finally:
            self.stop.set()
            deadline = time.monotonic() + self.settings.operation_timeout_seconds * 4
            for thread in self.threads: thread.join(max(0, deadline - time.monotonic()))
            if any(thread.is_alive() for thread in self.threads):
                raise TimeoutError('Consumer shutdown incomplete; next startup must recover transaction state.')
        if not self.errors.empty(): raise self.errors.get()
        self.progress(started)
        return 0
