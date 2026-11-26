# 进程内有界广播

生产者调用 `Broadcast(capacity=N).publish(bytes)` 取得从 0 开始的序号。
所有消费者共享同一环而各自持有独立读游标；`subscribe()` 只看后续事件，
`subscribe(start=序号)` 可读取仍保留的旧事件。保留空间满时覆盖最旧事件，发布不等待
慢订阅者。慢消费者收到 `Lagged(expected, oldest)`，须明确调用
`skip_lost()` 才会越过已丢序号；返回值为被跳过的半开区间。
总线关闭后拒绝发布和新订阅，现有订阅可消费剩余记录后收到 `Closed`。

自制样例在容量为 2 的总线上验证快慢订阅和显式跳过：

Python 3.9+，仅标准库。从仓库根目录运行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m pip check
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

预期快订阅者依次看到 `start, checkpoint, finish`；慢订阅者显式跳过
区间 `[0,1)` 后才读取 `checkpoint, finish`。一次 `recv` 成功即推进，
没有确认或重试协议；超时与缺口不推进。事件字节数默认不超过 65536。
仅进程内临时事件，序号不跨重启，不提供久存、主题过滤或分布式复制。
