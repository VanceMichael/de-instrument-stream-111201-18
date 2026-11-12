import argparse
import json
from pathlib import Path
import tempfile
import threading

from .domain import collect
from .storage import Store
from .streamstore import StreamStore
from .transport import Client, replay_server


def _render_status(store):
    status = store.status()
    for frame in status['frames']:
        raw = frame.pop('payload')
        try:
            frame['text'] = raw.decode('utf-8')
        except UnicodeDecodeError:
            frame['payload_hex'] = raw.hex()
    return status


def command_stream(arguments):
    store = StreamStore(arguments.db)
    try:
        print(json.dumps(_render_status(store), ensure_ascii=False, indent=2))
    finally:
        store.close()


def command_receive(arguments):
    from .receiver import run_receiver
    Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
    run_receiver(arguments.host, arguments.port, arguments.db,
                 buffer_limit=arguments.buffer_limit,
                 max_resyncs=arguments.max_resyncs,
                 max_queue=arguments.max_queue)


def command_simulate(arguments):
    from .simulator import run_simulation
    acks, logs, spec = run_simulation(
        arguments.host, arguments.port, arguments.fixture,
        mode=arguments.mode, garbage=arguments.garbage,
        coalesce=arguments.coalesce)
    for line in logs:
        print(line, flush=True)
    print('---')
    print(json.dumps({'收到确认': [list(item) for item in acks],
                      '确认数量': len(acks)}, ensure_ascii=False, indent=2))


def command_stream_demo(arguments):
    from .receiver import StreamReceiver
    from .simulator import run_simulation
    with tempfile.TemporaryDirectory() as directory:
        db_path = str(Path(directory) / 'stream.sqlite3')
        receiver = StreamReceiver('127.0.0.1', 0, db_path)
        thread = threading.Thread(target=receiver.serve_forever, daemon=True)
        thread.start()
        try:
            # 等待监听端口就绪。
            import time
            for _ in range(50):
                if receiver.port:
                    break
                time.sleep(0.02)
            acks, logs, spec = run_simulation(
                '127.0.0.1', receiver.port, arguments.fixture,
                mode=arguments.mode, garbage=arguments.garbage,
                coalesce=arguments.coalesce)
            for line in logs:
                print(line)
            receiver.shutdown()
            thread.join(timeout=5)
            store = StreamStore(db_path)
            try:
                print('---')
                print(json.dumps(_render_status(store), ensure_ascii=False, indent=2))
            finally:
                store.close()
        finally:
            receiver.shutdown()


def main():
    parser = argparse.ArgumentParser(description='本地采集与凭据查看')
    parser.add_argument('command',
                        choices=['demo', 'import', 'show', 'serve',
                                 'receive', 'simulate', 'stream', 'stream-demo'])
    parser.add_argument('--input', default='fixtures/sample.json')
    parser.add_argument('--db', default='var/records.sqlite3')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--fixture', default='fixtures/frames.json')
    parser.add_argument('--mode', choices=['faults', 'normal'], default='faults',
                        help='faults=按脚本注入坏帧与断线；normal=只发完好帧')
    parser.add_argument('--garbage', type=int, default=0,
                        help='故障注入：追加 N 个不含魔数的垃圾字节')
    parser.add_argument('--coalesce', action='store_true',
                        help='故障注入变体：把多帧合并为一次发送（粘包）')
    parser.add_argument('--buffer-limit', type=int, default=8192, dest='buffer_limit')
    parser.add_argument('--max-resyncs', type=int, default=16, dest='max_resyncs')
    parser.add_argument('--max-queue', type=int, default=128, dest='max_queue')
    arguments = parser.parse_args()

    if arguments.command == 'serve':
        from .api import serve
        serve(arguments.db, arguments.port)
        return
    if arguments.command == 'receive':
        command_receive(arguments)
        return
    if arguments.command == 'simulate':
        command_simulate(arguments)
        return
    if arguments.command == 'stream':
        command_stream(arguments)
        return
    if arguments.command == 'stream-demo':
        command_stream_demo(arguments)
        return
    if arguments.command == 'demo':
        with tempfile.TemporaryDirectory() as directory:
            payload = Path(arguments.input).read_bytes()
            with replay_server({'/source': [dict(body=payload.decode('utf-8'))]}) as (base, calls):
                status, headers, raw = Client().request(base + '/source')
                if status != 200:
                    raise RuntimeError('本地来源未返回成功响应')
                store = Store(Path(directory) / 'demo.sqlite3')
                rows = collect(json.loads(raw))
                digest = store.ingest('本地演示来源', raw, rows)
                print(json.dumps(dict(digest=digest, records=store.records(), requests=len(calls)), ensure_ascii=False, indent=2))
                store.close()
        return
    Path(arguments.db).parent.mkdir(parents=True, exist_ok=True)
    store = Store(arguments.db)
    try:
        if arguments.command == 'import':
            raw = Path(arguments.input).read_bytes()
            store.ingest(str(Path(arguments.input)), raw, collect(json.loads(raw)))
        print(json.dumps(store.records(), ensure_ascii=False, indent=2))
    finally:
        store.close()


if __name__ == '__main__':
    main()
