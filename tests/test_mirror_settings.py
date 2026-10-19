import tempfile
import unittest
from pathlib import Path

import yaml

from config.mirror_settings import load_mirror_settings, migrate_config
from config.settings import load_settings


def fixture_config():
    client = {'bootstrap.servers': 'localhost:9094', 'security.protocol': 'PLAINTEXT',
              'sasl.mechanism': '', 'sasl.jaas.config': '', 'username': '', 'password': '', 'topic': 'events'}
    return {'source': dict(client), 'destination': dict(client), 'mirror': {}}


class MirrorSettingsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'mirror.yml'

    def write(self, data):
        self.path.write_text(yaml.safe_dump(data), encoding='utf8')

    def test_defaults_and_source_loader_compatibility(self):
        self.write(fixture_config())
        settings = load_mirror_settings(self.path)
        self.assertEqual(settings.consumer_count, 3)
        self.assertEqual(settings.batch_max_records, 1)
        self.assertEqual(settings.source.to_client_config(), load_settings(self.path).kafka.to_client_config())

    def test_invalid_limits_and_credentials_are_rejected_without_secret(self):
        for value in (0, -1, float('nan'), float('inf'), True, '1'):
            data = fixture_config()
            data['mirror']['consumer_count'] = value
            self.write(data)
            with self.subTest(value=value), self.assertRaises(ValueError):
                load_mirror_settings(self.path)
        data = fixture_config()
        data['source'].update({'security.protocol': 'SASL_PLAINTEXT', 'password': 'fixture-secret'})
        self.write(data)
        with self.assertRaises(ValueError) as error:
            load_mirror_settings(self.path)
        self.assertNotIn('fixture-secret', str(error.exception))

    def test_migration_validates_then_deletes_old_config(self):
        legacy = self.path.with_name('config.yml')
        legacy.write_text(yaml.safe_dump({'kafka': fixture_config()['source']}), encoding='utf8')
        migrate_config(legacy, self.path, fixture_config()['destination'])
        self.assertFalse(legacy.exists())
        self.assertEqual(load_mirror_settings(self.path).source.topic, 'events')

    def test_migration_failure_preserves_old_and_does_not_overwrite(self):
        legacy = self.path.with_name('config.yml')
        legacy.write_text('kafka: invalid', encoding='utf8')
        with self.assertRaises(ValueError):
            migrate_config(legacy, self.path, fixture_config()['destination'])
        self.assertTrue(legacy.exists())
        self.assertFalse(self.path.exists())
        self.write(fixture_config())
        before = self.path.read_bytes()
        with self.assertRaises(FileExistsError):
            migrate_config(legacy, self.path, fixture_config()['destination'])
        self.assertEqual(before, self.path.read_bytes())
