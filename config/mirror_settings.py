"""Single local configuration for source, destination and mirror policy."""
from dataclasses import dataclass, replace
from pathlib import Path
import math
import yaml

from config.settings import KafkaSettings, parse_kafka, load_settings


@dataclass(frozen=True)
class MirrorSettings:
    source: KafkaSettings
    destination: KafkaSettings
    checkpoint_topic: str = '_mirror_product_view_state_v1'
    consumer_count: int = 3
    batch_max_records: int = 1
    batch_max_bytes: int = 8388608
    batch_wait_ms: int = 1000
    transaction_timeout_ms: int = 60000
    operation_timeout_seconds: float = 30
    metadata_refresh_seconds: float = 30
    retry_backoff_initial_ms: int = 500
    retry_backoff_max_ms: int = 10000
    progress_interval_seconds: float = 10
    max_records: int | None = None
    allow_shared_cluster_id: bool = False
    expected_source_topic_id: str | None = None

    def canary(self):
        return replace(self, destination=replace(self.destination, topic=self.destination.topic + '_mirror_canary'),
                       checkpoint_topic=self.checkpoint_topic.replace('_state_v1', '') + '_canary_state_v1')


def load_mirror_settings(path: str | Path | None = None) -> MirrorSettings:
    config_path = Path(path) if path else Path(__file__).with_name('mirror.yml')
    try:
        data = yaml.safe_load(config_path.read_text(encoding='utf8'))
    except yaml.YAMLError:
        raise ValueError('Invalid YAML in mirror configuration.') from None
    if not isinstance(data, dict) or not isinstance(data.get('mirror', {}), dict):
        raise ValueError('Mirror config must be a mapping.')
    source, destination = parse_kafka(data.get('source')), parse_kafka(data.get('destination'))
    options = data.get('mirror', {})
    allowed = set(MirrorSettings.__dataclass_fields__) - {'source', 'destination', 'max_records'}
    if set(options) - allowed:
        raise ValueError('Unknown mirror option.')
    settings = MirrorSettings(source, destination, **options)
    for name in allowed - {'checkpoint_topic', 'allow_shared_cluster_id', 'expected_source_topic_id'}:
        value = getattr(settings, name)
        expected = (int, float) if name.endswith('_seconds') else (int,)
        if isinstance(value, bool) or not isinstance(value, expected) or not math.isfinite(value) or value <= 0:
            raise ValueError(f'mirror.{name} must be finite and positive.')
    if not isinstance(settings.checkpoint_topic, str) or not settings.checkpoint_topic.strip():
        raise ValueError('mirror.checkpoint_topic must be a non-empty string.')
    if type(settings.allow_shared_cluster_id) is not bool:
        raise ValueError('mirror.allow_shared_cluster_id must be boolean.')
    if settings.expected_source_topic_id is not None and (not isinstance(settings.expected_source_topic_id, str) or not settings.expected_source_topic_id):
        raise ValueError('mirror.expected_source_topic_id must be a non-empty string.')
    if settings.checkpoint_topic == destination.topic:
        raise ValueError('Data and checkpoint topics must differ.')
    if settings.retry_backoff_initial_ms > settings.retry_backoff_max_ms:
        raise ValueError('Initial retry backoff exceeds maximum.')
    if settings.operation_timeout_seconds * 1000 >= settings.transaction_timeout_ms:
        raise ValueError('Operation deadline must be shorter than transaction timeout.')
    return settings


def migrate_config(legacy_path: Path, mirror_path: Path, destination: dict) -> None:
    """Validate in staging; never overwrite an existing mirror or lose the old input."""
    if mirror_path.exists():
        raise FileExistsError('Mirror configuration already exists; migration will not overwrite it.')
    original = load_settings(legacy_path).kafka
    data = yaml.safe_load(legacy_path.read_text(encoding='utf8'))
    staged = mirror_path.with_suffix('.yml.staging')
    try:
        with staged.open('x', encoding='utf8') as stream:
            yaml.safe_dump({'source': data['kafka'], 'destination': destination, 'mirror': {}}, stream, sort_keys=False)
        loaded = load_mirror_settings(staged)
        if loaded.source != original or load_settings(staged).kafka != original:
            raise ValueError('Source configuration changed during migration.')
        # Hard-link publication is exclusive: another writer cannot be overwritten.
        import os
        os.link(staged, mirror_path)
        try:
            legacy_path.unlink()
        except OSError:
            mirror_path.unlink()
            raise
    finally:
        staged.unlink(missing_ok=True)
