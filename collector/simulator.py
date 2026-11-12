"""本地仪器模拟端。

按 fixtures/frames.json 的事件脚本在 TCP 上回放协议字节流，
无需真实硬件即可核对接收器行为。支持：
- split_at：把一帧拆成任意分段发送（模拟网络分段）；
- corrupt_crc：发送 CRC 错误的帧（坏帧里也可能出现魔数样式的字节）；
- disconnect_before_ack：确认到达前断开，重连后 replay 补发；
- replay：重复帧，验证接收端去重；
- 新 epoch：设备重启代际，旧序号不得误去重；
- --garbage：额外注入无界垃圾字节，触发接收端重同步/断开；
- --coalesce：把后续帧合并在一次发送里（模拟粘包）。
"""
import json
import socket
import time

from .domain import MAGIC, encode_frame
from .receiver import ACK, ACK_TYPE


def load_scenario(path):
    with open(path, 'r', encoding='utf-8') as handle:
        spec = json.load(handle)
    if spec.get('magic_hex') != MAGIC.hex():
        raise ValueError('fixture 魔数与协议常量不一致')
    return spec


def _corrupt_frame(epoch, sequence, payload):
    raw = bytearray(encode_frame(epoch, sequence, payload))
    raw[-1] ^= 0xFF  # 翻转 CRC 尾字节，帧头仍带魔数
    return bytes(raw)


def build_ops(spec, mode):
    """把事件脚本展开为有序操作列表。

    op 形态：
      ('send', data, expect_ack, label, (epoch, sequence) | None)
      ('disconnect',)
      ('garbage', data)
    """
    ops = []
    coalesce = []

    def flush_coalesce():
        if coalesce:
            data = b''.join(item[0] for item in coalesce)
            labels = '|'.join(item[2] for item in coalesce)
            # 接收端逐帧落盘、逐帧确认；合并发送后依次收回每个 ACK。
            ops.append(('send', data, True, f'合并发送 {labels}',
                        [item[3] for item in coalesce]))
            coalesce.clear()

    for event in spec['events']:
        kind = event['type']
        identity = (event.get('epoch'), event.get('sequence'))
        if kind == 'frame':
            data = encode_frame(event['epoch'], event['sequence'],
                                event['text'].encode('utf-8'))
            label = f"帧 e{event['epoch']}s{event['sequence']}"
            if mode == 'coalesce' and not event.get('split_at'):
                coalesce.append((data, True, label, identity))
                continue
            flush_coalesce()
            parts = _split(data, event.get('split_at'))
            for index, chunk in enumerate(parts):
                # 接收端在整帧重组并落盘后只回一个 ACK，挂在最后一段上。
                last = index == len(parts) - 1
                ops.append(('send', chunk, last,
                            f'{label} 分段{index + 1}/{len(parts)}',
                            identity if last else None))
        elif kind == 'corrupt_crc':
            if mode == 'normal':
                continue  # 正常模式不注入坏帧
            flush_coalesce()
            ops.append(('send', _corrupt_frame(event['epoch'], event['sequence'], b''),
                        False, f"坏CRC帧 e{event['epoch']}s{event['sequence']}", None))
        elif kind == 'disconnect_before_ack':
            if mode == 'normal':
                continue
            flush_coalesce()
            ops.append(('disconnect',))
        elif kind == 'replay':
            flush_coalesce()
            data = encode_frame(event['epoch'], event['sequence'], '温度12'.encode('utf-8'))
            ops.append(('send', data, True,
                        f"补发重放 e{event['epoch']}s{event['sequence']}", identity))
        else:
            raise ValueError(f'未知事件类型: {kind}')
    flush_coalesce()
    return ops


def _split(data, split_at):
    if not split_at:
        return [data]
    parts = []
    start = 0
    for point in split_at:
        parts.append(data[start:point])
        start = point
    parts.append(data[start:])
    return [part for part in parts if part]


class InstrumentSimulator:
    def __init__(self, host, port, connect_timeout=5.0):
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout
        self.acks = []
        self.log = []
        self._sock = None

    def _connect(self):
        sock = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
        sock.settimeout(self.connect_timeout)
        self._sock = sock

    def _expect_ack(self, epoch, sequence, label):
        data = b''
        while len(data) < ACK.size:
            try:
                chunk = self._sock.recv(ACK.size - len(data))
            except socket.timeout:
                self.log.append(f'[未确认] {label} 等待 ACK 超时')
                return False
            if not chunk:
                self.log.append(f'[未确认] {label} 连接已关闭')
                return False
            data += chunk
        magic, ack_type, ack_epoch, ack_seq = ACK.unpack(data[:ACK.size])
        ok = magic == MAGIC and ack_type == ACK_TYPE and ack_epoch == epoch and ack_seq == sequence
        if ok:
            self.acks.append((ack_epoch, ack_seq))
            self.log.append(f'[已确认] {label}')
        else:
            self.log.append(f'[错误确认] {label} 收到 {data.hex()}')
        return ok

    def run(self, ops, reconnect_delay=0.2):
        """执行操作列表；断线后自动重连继续，返回 (确认列表, 日志)。"""
        connected = False
        for op in ops:
            if not connected:
                self._connect()
                connected = True
                self.log.append('[连接] 已建立到接收器连接')
            if op[0] == 'disconnect':
                self._close()
                connected = False
                self.log.append('[断线] 确认前主动断开，等待重连补发')
                time.sleep(reconnect_delay)
                continue
            if op[0] == 'garbage':
                self._send_garbage(op[1])
                continue
            _, data, expect_ack, label, identity = op
            self._sock.sendall(data)
            self.log.append(f'[发送] {label}（{len(data)} 字节）')
            if expect_ack:
                identities = identity if isinstance(identity, list) else [identity]
                for epoch, sequence in identities:
                    self._expect_ack(epoch, sequence, label)
        self._close()
        return self.acks, self.log

    def _send_garbage(self, junk):
        self.log.append(f'[故障注入] 在同一连接发送 {len(junk)} 字节无魔数垃圾')
        try:
            self._sock.sendall(junk)
        except OSError as error:
            self.log.append(f'[故障注入] 发送时连接已断开: {error}')
            return
        time.sleep(0.3)
        try:
            reply = self._sock.recv(16)
        except OSError as error:
            self.log.append(f'[故障注入] 接收器已按预期断开连接（{error}）')
            return
        if reply == b'':
            self.log.append('[故障注入] 接收器已按预期关闭连接（EOF）')
        else:
            self.log.append('[故障注入] 接收器仍保持连接（垃圾在重同步预算内被丢弃）')

    def _close(self):
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()
            self._sock = None


def run_simulation(host, port, fixture, mode, garbage=None, coalesce=False):
    spec = load_scenario(fixture)
    effective_mode = 'coalesce' if coalesce else mode
    ops = build_ops(spec, effective_mode)
    simulator = InstrumentSimulator(host, port)
    if garbage:
        junk = bytes((i % 251 + 1 for i in range(garbage)))
        ops.append(('garbage', junk))
    acks, logs = simulator.run(ops)
    return acks, simulator.log, spec
