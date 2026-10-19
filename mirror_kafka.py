"""Mirror retained Kafka records and continue following the source, without replay duplicates."""
import argparse
from dataclasses import replace
import json
import sys
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from config.mirror_settings import load_mirror_settings
from mirror.preflight import inspect_topology
from mirror.state import read_state, validate_resume
from mirror.worker import run_mirror


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', help='YAML config (default: config/mirror.yml)')
    parser.add_argument('--check', action='store_true', help='Read-only preflight; no group join, topic creation or producer fencing')
    parser.add_argument('--canary', action='store_true', help='Use separate destination, checkpoint and group identities')
    parser.add_argument('--max-records', type=int, help='Stop after this many committed records in this run')
    args = parser.parse_args(argv)
    if args.max_records is not None and args.max_records <= 0: parser.error('--max-records must be positive')
    settings = None
    try:
        settings = load_mirror_settings(args.config)
        if args.canary: settings = settings.canary()
        settings = replace(settings, max_records=args.max_records)
        if args.check:
            src = AdminClient({**settings.source.to_client_config(), 'log_level': 0})
            dst = AdminClient({**settings.destination.to_client_config(), 'log_level': 0})
            topology = inspect_topology(src, dst, settings)
            state = read_state(settings, topology) if topology['checkpoint_topic_id'] else None
            if state is not None: validate_resume(state, topology)
            if state is None and topology['destination_topic_id']:
                from count_messages import get_offsets
                bounds = get_offsets(dst, settings.destination.topic, settings.operation_timeout_seconds)
                if any(high != 0 for _, high in bounds.values()):
                    raise ValueError('Destination already written without matching mirror state.')
            print(json.dumps({'preflight': 'ok', 'source_cluster_id': topology['source_cluster_id'],
                              'destination_cluster_id': topology['destination_cluster_id'],
                              'partitions': topology['partitions'], 'source_bounds': topology['bounds'],
                              'destination_topic': settings.destination.topic,
                              'topics_to_create': [name for name, missing in ((settings.destination.topic, not topology['destination_topic_id']),
                                                 (settings.checkpoint_topic, not topology['checkpoint_topic_id'])) if missing],
                              'checkpoint': state.next_offsets if state else None}), flush=True)
            return 0
        return run_mirror(settings)
    except KeyboardInterrupt:
        print('Stopped; committed checkpoints are preserved.', file=sys.stderr)
        return 130
    except (KafkaException, OSError, ValueError, RuntimeError) as exc:
        message = str(exc) or f'{type(exc).__name__}: Kafka operation failed; check broker connectivity.'
        if settings:
            secrets = {settings.source.username, settings.source.password, settings.destination.username, settings.destination.password,
                       settings.source.sasl_jaas_config, settings.destination.sasl_jaas_config}
            for secret in sorted(secrets, key=len, reverse=True):
                if secret: message = message.replace(secret, '<redacted>')
        print('Error: ' + message, file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
