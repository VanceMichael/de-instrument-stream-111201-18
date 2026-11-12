"""长期运行的 TCP 帧接收器。

数据流方向：仪器 → socket → 读线程分帧 → 有界队列 → 工作线程落盘 → 确认。

可靠性要点：
- 队列有界：存储变慢时读线程在 put 处阻塞，TCP 接收窗口自然背压，
  绝不丢帧，也绝不在未持久化时确认。
- 确认严格发生在事务提交之后；进程在两者之间退出，重连后该帧被
  幂等重存并重新确认，仪器侧补发也不会多入库。
- 分帧致命错误（缓冲超限、重同步预算耗尽）立即断开并记录缺口。
"""
import json
import queue
import socket
import struct
import threading

from .domain import MAGIC
from .framing import (DEFAULT_BUFFER_LIMIT, DEFAULT_MAX_RESYNCS,
                      FrameParser, StreamError)
from .streamstore import StreamStore

ACK = struct.Struct('!2sHII')  # 魔数 + 类型 + epoch + 序号，共 12 字节
ACK_TYPE = 0x414B  # 'AK'
ACK_SIZE = ACK.size
DEFAULT_MAX_QUEUE = 128
_RECORD = 1
_SHUTDOWN = 2


class StreamReceiver:
    def __init__(self, host, port, db_path,
                 buffer_limit=DEFAULT_BUFFER_LIMIT,
                 max_resyncs=DEFAULT_MAX_RESYNCS,
                 max_queue=DEFAULT_MAX_QUEUE,
                 frame_hook=None):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.buffer_limit = buffer_limit
        self.max_resyncs = max_resyncs
        self.max_queue = max_queue
        # 测试钩子：帧落盘后、确认前回调，可模拟进程崩溃。
        self.frame_hook = frame_hook
        self._stop = threading.Event()
        self._connection_seq = 0
        self._active_conns = set()

    def shutdown(self):
        self._stop.set()

    def serve_forever(self):
        store = StreamStore(self.db_path)
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(8)
        server.settimeout(0.5)
        self.port = server.getsockname()[1]
        threads = []
        try:
            while not self._stop.is_set():
                try:
                    conn, address = server.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                self._connection_seq += 1
                thread = threading.Thread(
                    target=self._handle_connection,
                    args=(conn, address, self._connection_seq, store),
                    name=f'instrument-{self._connection_seq}', daemon=True)
                thread.start()
                threads.append(thread)
        finally:
            server.close()
            # 显式唤醒仍阻塞在 recv 上的连接线程。
            for conn in list(self._active_conns):
                try:
                    conn.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            for thread in threads:
                thread.join(timeout=5)
            store.close()

    def _handle_connection(self, conn, address, connection_id, store):
        conn.settimeout(1.0)
        self._active_conns.add(conn)
        parser = FrameParser(self.buffer_limit, self.max_resyncs)
        work = queue.Queue(maxsize=self.max_queue)
        last_sequence = {}  # epoch -> 本连接已确认处理的最大序号
        fatal_reason = None
        store.record_event(connection_id, 'connected',
                           json.dumps({'address': list(address)}, ensure_ascii=False))

        def reader():
            nonlocal fatal_reason
            try:
                while not self._stop.is_set():
                    try:
                        chunk = conn.recv(4096)
                    except socket.timeout:
                        continue
                    if not chunk:
                        break
                    try:
                        events = parser.feed(chunk)
                    except StreamError as error:
                        fatal_reason = str(error)
                        store.record_event(connection_id, 'fatal_stream_error',
                                           json.dumps({'error': str(error)},
                                                      ensure_ascii=False))
                        break
                    for event in events:
                        # 有界队列：满了就在此背压；停止时可被唤醒而不是永久阻塞。
                        while not self._stop.is_set():
                            try:
                                work.put((_RECORD, event), timeout=0.5)
                                break
                            except queue.Full:
                                continue
                        else:
                            break
            except OSError as error:
                store.record_event(connection_id, 'socket_error', str(error))
            finally:
                # 关闭路径下队列可能已满，非阻塞投递哨兵即可。
                try:
                    work.put_nowait((_SHUTDOWN, None))
                except queue.Full:
                    pass

        worker = threading.Thread(target=reader, name=f'reader-{connection_id}', daemon=True)
        worker.start()
        try:
            while not self._stop.is_set():
                try:
                    kind, event = work.get(timeout=0.5)
                except queue.Empty:
                    continue
                if kind == _SHUTDOWN:
                    break
                self._process(event, connection_id, store, conn, last_sequence)
        except (OSError, ConnectionError) as error:
            fatal_reason = fatal_reason or f'确认发送失败: {error}'
            store.record_event(connection_id, 'ack_send_failed', str(error))
        finally:
            worker.join(timeout=2)
            if fatal_reason is not None:
                # 只有无法恢复的断开才登记缺口；正常断开只留连接事件。
                for epoch, last in sorted(last_sequence.items()):
                    store.record_gap(
                        epoch, None, last, 'fatal_disconnect',
                        json.dumps({'connection_id': connection_id,
                                    'reason': fatal_reason,
                                    'address': list(address)}, ensure_ascii=False))
            store.record_event(
                connection_id, 'disconnected',
                json.dumps({'reason': fatal_reason or '正常断开',
                            'discarded_bytes': parser.discarded_total},
                           ensure_ascii=False))
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()
            self._active_conns.discard(conn)

    def _process(self, event, connection_id, store, conn, last_sequence):
        if event['kind'] == 'resync':
            store.record_event(connection_id, 'resync',
                               json.dumps(event, ensure_ascii=False, sort_keys=True))
            return
        epoch, sequence, payload = event['epoch'], event['sequence'], event['payload']
        previous = last_sequence.get(epoch)
        if previous is None:
            # 新代际的首个帧：与历史存储对比，识别跨连接遗留缺口。
            historical = store.max_sequence(epoch)
            if historical is not None and sequence > historical + 1:
                store.record_gap(
                    epoch, historical + 1, sequence - 1, 'missing_on_reconnect',
                    json.dumps({'connection_id': connection_id}, ensure_ascii=False))
        elif sequence > previous + 1:
            store.record_gap(
                epoch, previous + 1, sequence - 1, 'sequence_gap',
                json.dumps({'connection_id': connection_id,
                            'received': sequence}, ensure_ascii=False))
        # 1) 可靠保存（主键去重，新 epoch 不受旧序号影响）。
        inserted = store.save_frame(epoch, sequence, payload, connection_id)
        if previous is None or sequence > previous:
            last_sequence[epoch] = sequence
        # 钩子位于“事务已提交、确认未发出”的窗口，可安全模拟该窗口内的进程退出。
        if self.frame_hook is not None:
            self.frame_hook(epoch, sequence, inserted)
        # 2) 提交之后才确认；崩溃在这之前则重连补发、幂等重存。
        conn.sendall(ACK.pack(MAGIC, ACK_TYPE, epoch, sequence))
        store.record_ack(epoch, sequence, connection_id)


def run_receiver(host, port, db_path, **options):
    receiver = StreamReceiver(host, port, db_path, **options)

    def on_signal(signum, frame):
        receiver.shutdown()

    import signal
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    print(f'接收器启动，监听 {host}:{receiver.port}，数据库 {db_path}', flush=True)
    receiver.serve_forever()
    print('接收器已停止', flush=True)
