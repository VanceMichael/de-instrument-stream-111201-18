import unittest

from collector.domain import MAGIC, encode_frame
from collector.framing import (BufferLimitExceeded, FrameParser,
                               ResyncExhausted, StreamError)


class FrameParserTests(unittest.TestCase):
    def test_arbitrary_splitting_and_coalescing(self):
        frame1 = encode_frame(1, 1, '温度12'.encode())
        frame2 = encode_frame(1, 2, b'x' * 100)
        stream = frame1 + frame2
        # 逐字节喂入，模拟最极端的网络分段。
        parser = FrameParser()
        events = []
        for byte in stream:
            events.extend(parser.feed(bytes([byte])))
        self.assertEqual([(e['kind'], e['epoch'], e['sequence']) for e in events],
                         [('frame', 1, 1), ('frame', 1, 2)])
        # 两帧一次到达（粘包）。
        parser = FrameParser()
        events = parser.feed(stream)
        self.assertEqual(len(events), 2)

    def test_resync_after_corrupt_crc_with_embedded_magic(self):
        good = encode_frame(1, 1, b'ok')
        bad = bytearray(encode_frame(1, 2, b'bad'))
        bad[-1] ^= 0xFF  # CRC 失败，帧头仍带魔数
        after = encode_frame(1, 3, b'recovered')
        parser = FrameParser()
        events = parser.feed(good + bytes(bad) + after)
        kinds = [(e['kind'], e.get('sequence')) for e in events]
        self.assertIn(('frame', 1), kinds)
        self.assertIn(('frame', 3), kinds)
        self.assertTrue(any(e['kind'] == 'resync' for e in events))
        self.assertNotIn(('frame', 2), kinds)
        # 至少跳过坏帧头部的两个魔数字节，坏帧内部的伪魔数也会被继续跳过。
        self.assertGreaterEqual(parser.discarded_total, 2)

    def test_garbage_with_fake_magic_is_bounded(self):
        # 垃圾里出现看似魔数的字节，但其后长度字段非法。
        junk = b'\x01\x02' + MAGIC + b'\xff\xff' + b'\x03' * 10
        good = encode_frame(3, 7, b'alive')
        parser = FrameParser(buffer_limit=64)
        events = parser.feed(junk + good)
        self.assertEqual([(e['epoch'], e['sequence']) for e in events
                          if e['kind'] == 'frame'], [(3, 7)])
        self.assertGreaterEqual(parser.discarded_total, len(junk))

    def test_buffer_limit_raises_instead_of_growing(self):
        # 一次性喂入超过上限：缓冲超限，明确报错而不是无限增长。
        parser = FrameParser(buffer_limit=64)
        with self.assertRaises(BufferLimitExceeded):
            parser.feed(b'\x00' * 65)
        # 逐字节持续喂入不含魔数的数据：在重同步预算内即明确失败，
        # 同样不会静默囤积。
        parser = FrameParser(buffer_limit=64, max_resyncs=16)
        with self.assertRaises(StreamError):
            for _ in range(64):
                parser.feed(b'\x00')
        self.assertLessEqual(len(parser._buf), 64)

    def test_resync_budget_exhaustion(self):
        parser = FrameParser(buffer_limit=4096, max_resyncs=2)
        bad = bytearray(encode_frame(1, 1, b'x'))
        bad[-1] ^= 1
        with self.assertRaises(ResyncExhausted):
            parser.feed(bytes(bad) * 4)

    def test_oversized_length_field_resyncs(self):
        # 魔数合法但长度字段超过 4096，应丢弃魔数后重同步而不是信任长度。
        import struct
        header = struct.pack('!2sHII', MAGIC, 9000, 1, 1)
        good = encode_frame(1, 2, b'fine')
        parser = FrameParser()
        events = parser.feed(header + good)
        self.assertEqual([e['sequence'] for e in events if e['kind'] == 'frame'], [2])

    def test_half_magic_tail_is_retained(self):
        parser = FrameParser()
        first = parser.feed(b'\x00' * 5 + MAGIC[:1])
        self.assertEqual([e['kind'] for e in first], ['resync'])
        full = encode_frame(1, 1, b'p')
        events = parser.feed(MAGIC[1:] + full[2:])
        # 保留的半个魔数与后续字节拼成完整帧。
        self.assertEqual([e['sequence'] for e in events if e['kind'] == 'frame'], [1])


if __name__ == '__main__':
    unittest.main()
