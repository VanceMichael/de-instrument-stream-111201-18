"""有状态的仪器帧流分帧器。

处理任意分段或合并的 TCP 字节流，在有限范围内对坏帧重同步，
缓冲与重同步次数都有硬性上限，永远不会无上限囤积字节。
"""
import zlib

from .domain import CHECKSUM, HEADER, MAGIC

MAX_PAYLOAD = 4096
MAX_FRAME = HEADER.size + MAX_PAYLOAD + CHECKSUM.size
DEFAULT_BUFFER_LIMIT = 8192
DEFAULT_MAX_RESYNCS = 16


class StreamError(Exception):
    """字节流无法继续解析的致命错误。"""


class BufferLimitExceeded(StreamError):
    """待解析字节超过缓冲上限且找不到可信帧边界。"""


class ResyncExhausted(StreamError):
    """连续重同步次数超过预算，流已不可信。"""


class FrameParser:
    """逐块喂入字节，产出帧事件与重同步诊断事件。

    事件形态：
      {'kind': 'frame', 'epoch': int, 'sequence': int, 'payload': bytes}
      {'kind': 'resync', 'discarded': int, 'reason': str}

    无法在预算内恢复时抛出 StreamError 子类，调用方应断开连接并记录缺口。
    """

    def __init__(self, buffer_limit=DEFAULT_BUFFER_LIMIT, max_resyncs=DEFAULT_MAX_RESYNCS):
        if buffer_limit < 1:
            raise ValueError('缓冲上限必须为正')
        self.buffer_limit = buffer_limit
        self.max_resyncs = max_resyncs
        self._buf = bytearray()
        self._resync_streak = 0
        self.discarded_total = 0

    def feed(self, data):
        if not data:
            return []
        if len(self._buf) + len(data) > self.buffer_limit:
            raise BufferLimitExceeded(
                f'接收缓冲超过上限 {self.buffer_limit} 字节，拒绝继续囤积')
        self._buf.extend(data)
        events = []
        while self._buf:
            if self._buf[:2] != MAGIC:
                if not self._resync_to_magic(events):
                    break
                continue
            if len(self._buf) < HEADER.size:
                break  # 帧头尚未到齐，等待更多字节
            magic, size, epoch, sequence = HEADER.unpack(bytes(self._buf[:HEADER.size]))
            if size > MAX_PAYLOAD:
                # 长度字段非法：这个魔数不可信，跳过它继续寻找下一魔数。
                self._resync(2, f'长度字段 {size} 超过最大负载 {MAX_PAYLOAD}', events)
                continue
            frame_len = HEADER.size + size + CHECKSUM.size
            if len(self._buf) < frame_len:
                break  # 整帧未到齐
            raw = bytes(self._buf[:frame_len])
            expected = CHECKSUM.unpack(raw[-CHECKSUM.size:])[0]
            if zlib.crc32(raw[:-CHECKSUM.size]) != expected:
                self._resync(2, f'epoch={epoch} sequence={sequence} CRC 校验失败', events)
                continue
            del self._buf[:frame_len]
            self._resync_streak = 0
            events.append({'kind': 'frame', 'epoch': epoch, 'sequence': sequence,
                           'payload': raw[HEADER.size:-CHECKSUM.size]})
        return events

    def _resync_to_magic(self, events):
        index = self._buf.find(MAGIC)
        if index < 0:
            # 保留可能是半个魔数的末字节，其余丢弃；填满仍无魔数则致命。
            keep = 1 if self._buf[-1:] == MAGIC[:1] else 0
            dropped = len(self._buf) - keep
            if dropped:
                self._resync(dropped, '未找到魔数，丢弃无界垃圾字节', events)
            # 丢弃后仍然占满缓冲（例如末字节恰好是半个魔数），无法再接收数据。
            if len(self._buf) >= self.buffer_limit:
                raise BufferLimitExceeded(
                    f'整个 {self.buffer_limit} 字节缓冲内找不到魔数，断开连接')
            return False
        if index:
            self._resync(index, '丢弃帧间杂散字节后重新对齐魔数', events)
        return True

    def _resync(self, dropped, reason, events):
        del self._buf[:dropped]
        self.discarded_total += dropped
        self._resync_streak += 1
        events.append({'kind': 'resync', 'discarded': dropped, 'reason': reason})
        if self._resync_streak > self.max_resyncs:
            raise ResyncExhausted(
                f'连续 {self._resync_streak} 次重同步仍未恢复有效帧，判定流不可信')
        # 累计丢弃量超过缓冲上限：垃圾总量已不受控，即使分批到达也断开。
        if self.discarded_total > self.buffer_limit:
            raise BufferLimitExceeded(
                f'累计丢弃 {self.discarded_total} 字节仍无有效帧，'
                f'超过缓冲预算 {self.buffer_limit}，断开连接')
