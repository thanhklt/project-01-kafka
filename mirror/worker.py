"""Each transaction contains records from one source partition and its checkpoint."""
import time
from dataclasses import asdict
from confluent_kafka import KafkaException
from mirror.models import Origin, PreparedBatch
from mirror.state import encode


class RetryBatch(RuntimeError):
    """Transaction aborted definitively; seek to committed state before retrying."""


class OutcomeUnknown(RuntimeError):
    """Stop and fence/restore on next startup. Never blindly reproduce this batch."""


def prepare_batch(messages, source_cluster_id):
    if not messages or len({m.partition() for m in messages}) != 1:
        raise ValueError('A transaction must contain exactly one source partition.')
    if any(a.offset() >= b.offset() for a, b in zip(messages, messages[1:])):
        raise ValueError('Source offsets must increase within a batch.')
    records = []
    for message in messages:
        headers = list(message.headers() or [])
        if any(name == '__mirror_origin_v1' for name, _ in headers):
            raise ValueError('Reserved origin header already present on source record.')
        origin = Origin(source_cluster_id, message.topic(), message.partition(), message.offset())
        headers.append(('__mirror_origin_v1', encode(asdict(origin))))
        record = dict(partition=message.partition(), key=message.key(), value=message.value(), headers=headers)
        _, timestamp = message.timestamp()
        if timestamp is not None and timestamp >= 0: record['timestamp'] = timestamp
        records.append(record)
    return PreparedBatch(records, {messages[-1].partition(): messages[-1].offset() + 1})


def retry_operation(operation, settings):
    deadline = time.monotonic() + settings.operation_timeout_seconds
    backoff = settings.retry_backoff_initial_ms / 1000
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0: raise OutcomeUnknown('Transaction outcome unknown; restart to recover checkpoint.')
        try:
            return operation(remaining)
        except KafkaException as exc:
            if not exc.args[0].retriable(): raise
            time.sleep(min(backoff, max(0, deadline - time.monotonic())))
            backoff = min(backoff * 2, settings.retry_backoff_max_ms / 1000)


def produce_bounded(producer, settings, **record):
    deadline = time.monotonic() + settings.operation_timeout_seconds
    while True:
        try:
            producer.produce(**record)
            return
        except BufferError:
            if time.monotonic() >= deadline: raise TimeoutError('Producer queue remained full.')
            producer.poll(.05)


def commit_records(producer, records, settings):
    producer.begin_transaction()
    committing = False
    try:
        for record in records:
            produce_bounded(producer, settings, **record)
        committing = True
        retry_operation(lambda remaining: producer.commit_transaction(remaining), settings)
    except KafkaException as exc:
        if exc.args[0].txn_requires_abort():
            retry_operation(lambda remaining: producer.abort_transaction(remaining), settings)
            raise RetryBatch('Transaction aborted; replay from committed checkpoint.') from exc
        # Fatal/fenced errors must not be hidden by cleanup or retried as a new producer.
        raise
    except OutcomeUnknown:
        raise
    except Exception:
        if not committing:
            retry_operation(lambda remaining: producer.abort_transaction(remaining), settings)
        raise


def commit_batch(producer, batch, state, settings):
    records = [dict(topic=settings.destination.topic, **record) for record in batch.records]
    records.extend(dict(topic=settings.checkpoint_topic, partition=0, key=f'p:{partition}'.encode(),
                        value=encode({'version': 1, 'next_offset': offset}))
                   for partition, offset in batch.next_offsets.items())
    commit_records(producer, records, settings)
    state.next_offsets.update(batch.next_offsets)


def run_mirror(settings):
    from mirror.group import Supervisor
    return Supervisor(settings).run()
