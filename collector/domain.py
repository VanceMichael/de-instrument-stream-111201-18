import base64
import struct
import zlib

MAGIC = b'\xa5\x5a'
HEADER = struct.Struct('!2sHII')
CHECKSUM = struct.Struct('!I')


def encode_frame(epoch, sequence, payload):
    if len(payload) > 4096:
        raise ValueError('帧负载超长')
    header = HEADER.pack(MAGIC, len(payload), epoch, sequence)
    body = header + payload
    return body + CHECKSUM.pack(zlib.crc32(body))


def decode_frame(raw):
    if len(raw) < HEADER.size + CHECKSUM.size:
        raise ValueError('帧不完整')
    magic, size, epoch, sequence = HEADER.unpack(raw[:HEADER.size])
    if magic != MAGIC or size > 4096 or len(raw) != HEADER.size + size + CHECKSUM.size:
        raise ValueError('帧头或长度不符')
    expected = CHECKSUM.unpack(raw[-CHECKSUM.size:])[0]
    if zlib.crc32(raw[:-CHECKSUM.size]) != expected:
        raise ValueError('校验失败')
    return dict(epoch=epoch, sequence=sequence, payload=raw[HEADER.size:-CHECKSUM.size])


def collect(payload):
    result = []
    for value in payload['readings']:
        raw = encode_frame(value['epoch'], value['sequence'], value['text'].encode('utf-8'))
        parsed = decode_frame(raw)
        result.append(dict(key=f"{parsed['epoch']}:{parsed['sequence']}",
                           epoch=parsed['epoch'], sequence=parsed['sequence'], text=parsed['payload'].decode('utf-8')))
    return result
