"""仪器帧的持久化存储。

关键保证：
- 帧以 (epoch, sequence) 为主键，重连补发与重复帧不会多入库；
  设备重启后的新 epoch 与旧序号互不干扰。
- 确认只在帧事务提交之后登记，进程在保存与确认之间退出时，
  重连后同一帧会被重新保存（幂等）并重新确认。
- 缺口与分帧诊断事件一并落盘，供事后核对乱序补发到底收到了什么。
"""
import json
import sqlite3
import threading
from datetime import datetime, timezone


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')


class StreamStore:
    def __init__(self, path):
        # 接收器在独立连接线程中使用同一个存储；写操作由 _lock 串行化。
        self.connection = sqlite3.connect(path, timeout=10, isolation_level=None,
                                          check_same_thread=False)
        self._lock = threading.RLock()
        self.connection.row_factory = sqlite3.Row
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.execute('PRAGMA journal_mode=WAL')
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS stream_frames(
                epoch INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                payload BLOB NOT NULL,
                received_at TEXT NOT NULL,
                PRIMARY KEY(epoch, sequence));
            CREATE TABLE IF NOT EXISTS stream_acks(
                epoch INTEGER NOT NULL,
                sequence INTEGER NOT NULL,
                acked_at TEXT NOT NULL,
                connection_id INTEGER NOT NULL,
                PRIMARY KEY(epoch, sequence));
            CREATE TABLE IF NOT EXISTS stream_gaps(
                id INTEGER PRIMARY KEY,
                epoch INTEGER NOT NULL,
                expected_sequence INTEGER,
                last_sequence INTEGER,
                kind TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS stream_events(
                id INTEGER PRIMARY KEY,
                connection_id INTEGER,
                kind TEXT NOT NULL,
                detail TEXT NOT NULL,
                created_at TEXT NOT NULL);
        """)

    def save_frame(self, epoch, sequence, payload, connection_id):
        """幂等保存一帧，返回 True 表示新入库、False 表示重复帧。"""
        with self._lock, self.connection:
            cursor = self.connection.execute(
                'INSERT OR IGNORE INTO stream_frames VALUES (?,?,?,?)',
                (epoch, sequence, bytes(payload), _now()))
            inserted = cursor.rowcount > 0
            self.connection.execute(
                'INSERT INTO stream_events(connection_id,kind,detail,created_at)'
                ' VALUES (?,?,?,?)',
                (connection_id, 'frame_stored' if inserted else 'frame_duplicate',
                 json.dumps({'epoch': epoch, 'sequence': sequence},
                            ensure_ascii=False, sort_keys=True), _now()))
        return inserted

    def record_ack(self, epoch, sequence, connection_id):
        """帧已可靠保存后调用；重复确认只登记一次。"""
        with self._lock, self.connection:
            self.connection.execute(
                'INSERT OR IGNORE INTO stream_acks VALUES (?,?,?,?)',
                (epoch, sequence, _now(), connection_id))

    def record_gap(self, epoch, expected_sequence, last_sequence, kind, detail):
        with self._lock, self.connection:
            self.connection.execute(
                'INSERT INTO stream_gaps(epoch,expected_sequence,last_sequence,kind,detail,created_at)'
                ' VALUES (?,?,?,?,?,?)',
                (epoch, expected_sequence, last_sequence, kind, detail, _now()))

    def record_event(self, connection_id, kind, detail):
        with self._lock, self.connection:
            self.connection.execute(
                'INSERT INTO stream_events(connection_id,kind,detail,created_at)'
                ' VALUES (?,?,?,?)',
                (connection_id, kind, detail, _now()))

    def has_ack(self, epoch, sequence):
        with self._lock:
            row = self.connection.execute(
                'SELECT 1 FROM stream_acks WHERE epoch=? AND sequence=?',
                (epoch, sequence)).fetchone()
        return row is not None

    def max_sequence(self, epoch):
        with self._lock:
            row = self.connection.execute(
                'SELECT max(sequence) FROM stream_frames WHERE epoch=?', (epoch,)).fetchone()
        return row[0]

    def frames(self):
        with self._lock:
            return [dict(row) for row in self.connection.execute(
                'SELECT epoch, sequence, payload, received_at FROM stream_frames '
                'ORDER BY epoch, sequence')]

    def acks(self):
        with self._lock:
            return [dict(row) for row in self.connection.execute(
                'SELECT epoch, sequence, acked_at, connection_id FROM stream_acks '
                'ORDER BY epoch, sequence')]

    def gaps(self):
        with self._lock:
            return [dict(row) for row in self.connection.execute(
                'SELECT id, epoch, expected_sequence, last_sequence, kind, detail, created_at '
                'FROM stream_gaps ORDER BY id')]

    def events(self):
        with self._lock:
            return [dict(row) for row in self.connection.execute(
                'SELECT id, connection_id, kind, detail, created_at '
                'FROM stream_events ORDER BY id')]

    def status(self):
        frames = self.frames()
        acks = self.acks()
        acked = {(row['epoch'], row['sequence']) for row in acks}
        unacked = [{'epoch': row['epoch'], 'sequence': row['sequence']}
                   for row in frames if (row['epoch'], row['sequence']) not in acked]
        return {
            'frame_count': len(frames),
            'ack_count': len(acks),
            'gap_count': len(self.gaps()),
            'event_count': len(self.events()),
            'unacked_frames': unacked,
            'frames': frames,
            'acks': acks,
            'gaps': self.gaps(),
            'events': self.events(),
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()
        return False

    def close(self):
        self.connection.close()
