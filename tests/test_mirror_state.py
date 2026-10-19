import unittest

from mirror.models import MirrorState
from mirror.state import decode_state, encode, manifest_for, validate_resume


def topology():
    return {'source_cluster_id': 'source', 'source_topic_id': 's-id', 'source_topic': 'events',
            'destination_cluster_id': 'target', 'destination_topic_id': 'd-id',
            'destination_topic': 'events', 'checkpoint_topic_id': 'c-id',
            'partitions': [0, 1], 'bounds': {0: (100, 150), 1: (20, 27)}}


class MirrorStateTests(unittest.TestCase):
    def state(self):
        return MirrorState(manifest_for(topology()), {0: 101, 1: 20})

    def test_roundtrip_and_sparse_offsets(self):
        state = self.state()
        records = [(b'manifest', encode(state.manifest)),
                   (b'p:0', encode({'version': 1, 'next_offset': 101})),
                   (b'p:0', encode({'version': 1, 'next_offset': 105})),
                   (b'p:1', encode({'version': 1, 'next_offset': 20}))]
        result = decode_state(records)
        self.assertEqual(result.next_offsets, {0: 105, 1: 20})
        validate_resume(result, topology())

    def test_missing_corrupt_recreated_or_out_of_range_state_fails_closed(self):
        for change in ('missing', 'low', 'high', 'recreated', 'schema'):
            state = self.state()
            if change == 'missing': del state.next_offsets[1]
            if change == 'low': state.next_offsets[0] = 99
            if change == 'high': state.next_offsets[0] = 151
            if change == 'recreated': state.manifest['source_topic_id'] = 'other'
            if change == 'schema': state.manifest['version'] = 9
            with self.subTest(change=change), self.assertRaises(ValueError):
                validate_resume(state, topology())
        with self.assertRaises(ValueError):
            decode_state([(b'manifest', b'bad json')])
        with self.assertRaises(ValueError):
            decode_state([(b'p:0', None)])

    def test_initial_manifest_requires_fully_propagated_topic_ids(self):
        with self.assertRaises(ValueError):
            manifest_for(topology() | {'checkpoint_topic_id': None})
