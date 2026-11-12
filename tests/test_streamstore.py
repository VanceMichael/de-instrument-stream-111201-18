import tempfile
import unittest
from pathlib import Path

from collector.streamstore import StreamStore


class StreamStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temporary.name) / 'stream.sqlite3'
        self.store = StreamStore(self.db_path)

    def tearDown(self):
        self.store.close()
        self.temporary.cleanup()

    def test_frame_idempotent_by_epoch_sequence(self):
        self.assertTrue(self.store.save_frame(1, 1, b'a', 1))
        # 重连补发 / 重复帧不产生新行。
        self.assertFalse(self.store.save_frame(1, 1, b'a', 2))
        self.assertEqual(len(self.store.frames()), 1)

    def test_new_epoch_is_not_deduped_by_old_sequence(self):
        self.store.save_frame(1, 1, b'old', 1)
        # 设备重启后的新代际，序号从 1 重新开始，必须作为新帧入库。
        self.assertTrue(self.store.save_frame(2, 1, b'new', 1))
        keys = {(row['epoch'], row['sequence']) for row in self.store.frames()}
        self.assertEqual(keys, {(1, 1), (2, 1)})

    def test_ack_only_recorded_after_save(self):
        self.store.save_frame(1, 1, b'a', 1)
        self.assertFalse(self.store.has_ack(1, 1))
        self.store.record_ack(1, 1, 1)
        self.assertTrue(self.store.has_ack(1, 1))
        # 重复确认只登记一次。
        self.store.record_ack(1, 1, 2)
        self.assertEqual(len(self.store.acks()), 1)

    def test_gap_and_event_records(self):
        self.store.record_gap(1, 3, 5, 'sequence_gap', '{"missing":[3,4,5]}')
        self.store.record_event(1, 'resync', '{"discarded":2}')
        self.assertEqual(self.store.gaps()[0]['kind'], 'sequence_gap')
        self.assertEqual(self.store.events()[0]['kind'], 'resync')

    def test_reconnect_after_crash_between_save_and_ack(self):
        # 模拟进程在保存之后、确认之前退出：帧已落盘但没有 ack。
        self.store.save_frame(1, 1, b'a', 1)
        self.store.close()
        reopened = StreamStore(self.db_path)
        try:
            self.assertFalse(reopened.has_ack(1, 1))
            # 仪器重连补发同一帧：幂等重存成功，随后可以安全补确认。
            inserted = reopened.save_frame(1, 1, b'a', 2)
            self.assertFalse(inserted)
            reopened.record_ack(1, 1, 2)
            self.assertEqual(len(reopened.frames()), 1)
            self.assertTrue(reopened.has_ack(1, 1))
        finally:
            reopened.close()

    def test_status_surfaces_unacked(self):
        self.store.save_frame(1, 1, b'a', 1)
        self.store.save_frame(1, 2, b'b', 1)
        self.store.record_ack(1, 1, 1)
        status = self.store.status()
        self.assertEqual(status['unacked_frames'], [{'epoch': 1, 'sequence': 2}])


if __name__ == '__main__':
    unittest.main()
