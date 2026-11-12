import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path

from collector.domain import MAGIC, encode_frame
from collector.receiver import ACK, ACK_TYPE, StreamReceiver
from collector.simulator import InstrumentSimulator, build_ops, load_scenario
from collector.streamstore import StreamStore


class ReceiverHarness:
    def __init__(self, **options):
        self.temporary = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temporary.name) / 'stream.sqlite3')
        self.receiver = StreamReceiver('127.0.0.1', 0, self.db_path, **options)
        self.thread = threading.Thread(target=self.receiver.serve_forever, daemon=True)
        self.thread.start()
        for _ in range(50):
            if self.receiver.port:
                break
            time.sleep(0.02)

    def store(self):
        return StreamStore(self.db_path)

    def shutdown(self):
        self.receiver.shutdown()
        self.thread.join(timeout=5)
        self.temporary.cleanup()


def send_raw(port, data):
    with socket.create_connection(('127.0.0.1', port), timeout=5) as sock:
        sock.sendall(data)


def recv_ack(sock):
    data = b''
    while len(data) < ACK.size:
        chunk = sock.recv(ACK.size - len(data))
        if not chunk:
            raise ConnectionError('连接在确认前关闭')
        data += chunk
    return ACK.unpack(data)


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.harness = ReceiverHarness()

    def tearDown(self):
        self.harness.shutdown()

    def test_split_frames_acked_after_durable_save(self):
        frame = encode_frame(1, 1, '温度12'.encode())
        with socket.create_connection(('127.0.0.1', self.harness.receiver.port), timeout=5) as sock:
            # 在 split_at=[1,3,8] 处逐段发送。
            marks = [0, 1, 3, 8, len(frame)]
            for start, end in zip(marks, marks[1:]):
                sock.sendall(frame[start:end])
                time.sleep(0.01)
            magic, ack_type, epoch, sequence = recv_ack(sock)
        self.assertEqual((magic, ack_type, epoch, sequence),
                         (MAGIC, ACK_TYPE, 1, 1))
        with self.harness.store() as store:
            self.assertEqual(len(store.frames()), 1)
            self.assertTrue(store.has_ack(1, 1))

    def test_corrupt_frame_resyncs_then_good_frame_accepted(self):
        bad = bytearray(encode_frame(1, 2, b''))
        bad[-1] ^= 0xFF
        good = encode_frame(1, 3, b'ok')
        with socket.create_connection(('127.0.0.1', self.harness.receiver.port), timeout=5) as sock:
            sock.sendall(bytes(bad) + good)
            magic, _, epoch, sequence = recv_ack(sock)
        self.assertEqual((epoch, sequence), (1, 3))
        with self.harness.store() as store:
            self.assertEqual([(row['epoch'], row['sequence']) for row in store.frames()],
                             [(1, 3)])
            self.assertTrue(any(row['kind'] == 'resync' for row in store.events()))

    def test_replay_and_reconnect_duplicates_ingest_once(self):
        frame = encode_frame(1, 1, '温度12'.encode())
        for _ in range(2):  # 两次独立连接，模拟断线补发与重放
            with socket.create_connection(('127.0.0.1', self.harness.receiver.port), timeout=5) as sock:
                sock.sendall(frame)
                recv_ack(sock)
        with self.harness.store() as store:
            self.assertEqual(len(store.frames()), 1)
            self.assertEqual(len(store.acks()), 1)
            self.assertTrue(any(row['kind'] == 'frame_duplicate' for row in store.events()))

    def test_new_epoch_restarts_sequence_space(self):
        with socket.create_connection(('127.0.0.1', self.harness.receiver.port), timeout=5) as sock:
            sock.sendall(encode_frame(1, 1, b'old'))
            recv_ack(sock)
        with socket.create_connection(('127.0.0.1', self.harness.receiver.port), timeout=5) as sock:
            sock.sendall(encode_frame(2, 1, '新启动'.encode()))
            recv_ack(sock)
        with self.harness.store() as store:
            self.assertEqual(len(store.frames()), 2)
            self.assertEqual(store.max_sequence(2), 1)

    def test_fixture_scenario_end_to_end_with_disconnect_replay(self):
        spec = load_scenario('fixtures/frames.json')
        ops = build_ops(spec, 'faults')
        simulator = InstrumentSimulator('127.0.0.1', self.harness.receiver.port)
        acks, logs = simulator.run(ops)
        # 完好帧 e1s1（含重放一次）、e2s1 共三个帧实例，但只入库两条。
        self.assertEqual(sorted(acks), [(1, 1), (1, 1), (2, 1)])
        with self.harness.store() as store:
            keys = {(row['epoch'], row['sequence']) for row in store.frames()}
            self.assertEqual(keys, {(1, 1), (2, 1)})
            self.assertTrue(any(row['kind'] == 'resync' for row in store.events()))
            # 坏帧 e1s2 不得入库。
            self.assertNotIn((1, 2), keys)

    def test_garbage_flood_disconnects_and_records_gap(self):
        with socket.create_connection(('127.0.0.1', self.harness.receiver.port), timeout=5) as sock:
            sock.settimeout(2)
            sock.sendall(encode_frame(1, 1, b'before'))
            recv_ack(sock)
            # 8192 字节内不含魔数，累计丢弃超预算，触发致命断开。
            sock.sendall(bytes((i % 251 + 1 for i in range(8200))))
            try:
                reply = sock.recv(32)
            except OSError:
                reply = b''
            self.assertEqual(reply, b'')  # 服务端关闭连接，read 返回 EOF
        time.sleep(0.3)
        with self.harness.store() as store:
            gaps = store.gaps()
            self.assertTrue(any(row['kind'] == 'fatal_disconnect' for row in gaps))
            self.assertTrue(any(row['kind'] == 'fatal_stream_error'
                                for row in store.events()))

    def test_backpressure_blocks_without_dropping(self):
        # max_queue=1 + 钩子阻塞，制造存储变慢；发送必须被背压而不是丢帧。
        gate = threading.Event()
        seen = []
        reached = threading.Event()

        def hook(epoch, sequence, inserted):
            seen.append(sequence)
            if sequence == 2:
                reached.set()
                gate.wait(5)

        harness = ReceiverHarness(max_queue=1, frame_hook=hook)
        try:
            with socket.create_connection(('127.0.0.1', harness.receiver.port), timeout=5) as sock:
                sock.settimeout(5)
                sender = threading.Thread(
                    target=lambda: [sock.sendall(encode_frame(1, i, b'x'))
                                    for i in range(1, 7)], daemon=True)
                sender.start()
                # 存储在 s2 处被卡住：有界队列满后发送端被背压，没有任何帧被丢弃。
                self.assertTrue(reached.wait(5))
                gate.set()
                sender.join(5)
                for _ in range(6):
                    recv_ack(sock)
            with harness.store() as store:
                self.assertEqual(sorted(seen), [1, 2, 3, 4, 5, 6])
                self.assertEqual(len(store.frames()), 6)
        finally:
            harness.shutdown()

    def test_crash_between_save_and_ack_safely_reconnects(self):
        # 第二个帧落盘后、确认前模拟进程崩溃（抛错打断连接处理，只触发一次）。
        fired = {'done': False}

        def hook(epoch, sequence, inserted):
            if sequence == 2 and not fired['done']:
                fired['done'] = True
                raise RuntimeError('模拟进程在保存与确认之间退出')

        harness = ReceiverHarness(frame_hook=hook)
        try:
            with socket.create_connection(('127.0.0.1', harness.receiver.port), timeout=5) as sock:
                sock.settimeout(2)
                sock.sendall(encode_frame(1, 1, b'one'))
                recv_ack(sock)
                sock.sendall(encode_frame(1, 2, b'two'))
                with self.assertRaises((OSError, ConnectionError)):
                    sock.recv(32)
            # 重连补发 s2（幂等），并继续 s3。
            with socket.create_connection(('127.0.0.1', harness.receiver.port), timeout=5) as sock:
                sock.sendall(encode_frame(1, 2, b'two'))
                recv_ack(sock)
                sock.sendall(encode_frame(1, 3, b'three'))
                recv_ack(sock)
            with harness.store() as store:
                self.assertEqual(len(store.frames()), 3)
                self.assertTrue(store.has_ack(1, 2))
        finally:
            harness.shutdown()

    def test_sequence_gap_is_recorded(self):
        with socket.create_connection(('127.0.0.1', self.harness.receiver.port), timeout=5) as sock:
            sock.sendall(encode_frame(1, 1, b'a'))
            recv_ack(sock)
            sock.sendall(encode_frame(1, 4, b'd'))
            recv_ack(sock)
        with self.harness.store() as store:
            gap = store.gaps()[0]
            self.assertEqual((gap['epoch'], gap['expected_sequence'],
                              gap['last_sequence']), (1, 2, 3))


if __name__ == '__main__':
    unittest.main()
