# 仪器数据流·有界重连接收

面向合作资料接收人员的 Python 采集组件。编码和校验完整仪器帧，协议使用大端长度、启动代际、序号与 CRC32。
输入样本都是本地合成数据，example 域名只表示来源身份，不需联网访问。

## 安装与运行

使用 Python 3.11，依赖仅为标准库，SQLite 随解释器提供，无需安装第三方包。
从项目根目录执行；演示启动真实的回环 HTTP 来源、处理响应、事务入库后读取实际结果。
每次演示使用独立临时目录，结束后清理；通常两秒内结束，不回放历史输出。

```bash
python3 -m compileall -q collector
python3 -m unittest discover -s tests -v
python3 -m collector demo
python3 -m collector stream-demo
```

常规导入保留在本地数据库中，可重复运行；默认路径已忽略，不会进入版本库。

```bash
python3 -m collector import --input fixtures/sample.json --db var/records.sqlite3
python3 -m collector show --db var/records.sqlite3
python3 -m collector serve --db var/records.sqlite3 --port 8080
```

查询服务监听回环地址，`GET /records` 返回数据库中的记录；按 Ctrl-C 停止。

## 连续仪器流接收

`receive` 是可以长期开着的 TCP 接收器，配合 `simulate` 本地仪器模拟端使用，
全程只走回环地址，不需要真实硬件。协议帧为魔数 `a55a` + 大端长度 + 启动代际
epoch + 序号 sequence + 负载 + CRC32，确认帧为魔数 + 类型 `AK` + epoch + 序号。

```bash
# 终端一：长期接收器（Ctrl-C / SIGTERM 优雅退出）
python3 -m collector receive --db var/stream.sqlite3 --port 9191

# 终端二：按 fixtures/frames.json 回放事件（分段、坏 CRC、断线补发、重放、新代际）
python3 -m collector simulate --port 9191

# 查看帧级状态、确认记录、缺口与分帧事件
python3 -m collector stream --db var/stream.sqlite3
```

模拟端的运行方式：

```bash
# 故障注入（默认）：完整事件脚本，含坏帧与确认前断线
python3 -m collector simulate --port 9191
# 正常读取：跳过坏帧与断线，只发完好帧
python3 -m collector simulate --port 9191 --mode normal
# 粘包变体：多帧合并为一次发送
python3 -m collector simulate --port 9191 --coalesce
# 额外垃圾洪泛：同一连接追加 N 个不含魔数的字节，验证有界断开与缺口登记
python3 -m collector simulate --port 9191 --mode normal --garbage 9000
# 单进程自演：接收器与模拟端在临时目录内跑完即清理，并打印完整状态
python3 -m collector stream-demo
```

接收器保证：最大帧长 4096 字节、接收缓冲上限 8192 字节（可用
`--buffer-limit/--max-resyncs/--max-queue` 调整）；坏帧在有限范围内按魔数
重同步，缓冲超限或重同步预算耗尽时明确断开并在 `stream_gaps` 记录
`fatal_disconnect` 缺口，绝不无上限囤积字节。帧以 `(epoch, sequence)` 为主键
幂等入库，重连补发与重复帧不会多入库；设备重启产生的新 epoch 序号空间独立，
不会被旧序号去重。确认严格发生在事务提交之后：存储变慢时有界队列产生背压，
不丢帧也不提前确认；进程在保存与确认之间退出，重连后该帧幂等重存并重新确认。

## 组件与数据

`collector/domain.py` 负责来源格式和领域校验；`storage.py` 保留原始对象、导入凭据和当前记录。
单次导入在同一事务内提交，异常时整体回滚；相同来源和原文只登记一次凭据。
当前记录以领域标识为键，同键再次导入替换当前观察值，不代表保留了完整修订历史。
`transport.py` 提供请求客户端和本地响应服务；服务按路径依次消费响应脚本，并记录实际请求。
`api.py` 提供只读查询入口。`fixtures/sample.json` 是当前导入器支持的输入。
`framing.py` 是有状态流分帧器（有界缓冲、魔数重同步、CRC 校验、致命错误上抛）；
`streamstore.py` 管理帧、确认、缺口与事件四张表；`receiver.py` 是 TCP 接收器
（读线程分帧、有界队列背压、工作线程先落盘后确认、断线重连）；
`simulator.py` 按 `fixtures/frames.json` 事件脚本驱动本地仪器模拟端。
其他附件描述同领域的合成页面、协议或事件，可以由调用方构造本地来源。
当前响应服务是顺序脚本工具，不自动推断认证、文件或表单业务；领域对象提供对应的本地交互。

## 可复现运行

宿主机或容器均不需要网络。镜像准备好以后可以关闭外部网络再运行上述命令。
仓库内不含真实账号、新闻正文或人员资料；不要将生产凭据写入样本或日志。
