import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        raw = encode_frame(1, 2, b'data')
        self.assertEqual(decode_frame(raw)['payload'], b'data')
        with self.assertRaises(ValueError):
            decode_frame(raw[:-1])
        broken = bytearray(raw)
        broken[-1] ^= 1
        with self.assertRaises(ValueError):
            decode_frame(bytes(broken))

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
