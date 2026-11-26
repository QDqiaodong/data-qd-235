# data-qd-235 · 进程内服务端点发现

服务发布方维护当前的服务端点集合，调用方各自维护一份可直接用于连接的本地视图。
目录以有界广播传递变化，但**不把广播当可靠消息队列**：慢客户端的某条变更被覆盖时，
它从权威目录重建当前视图，再从同一切点继续读增量。即使已撤销的旧端点仍能接收连接，
重建后的查找也不会再把它选出来。

## 运行

Python 3.9+，仅标准库。从仓库根目录运行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install --no-index -r requirements.txt
.venv/bin/python -m pip check
.venv/bin/python -m unittest -v
.venv/bin/python demo.py
```

演示启动两个真实回环 HTTP 服务，不访问外网，使用自制内容。容量为 2 的广播环里，
快客户端应用一条更新，慢客户端错过同一条更新并从快照恢复。旧服务全程在线，
请求改变是因为客户端查找结果不同，不是健康检查、连接失败或重试。

```text
before: (200, 'old-renderer')
retained replacement: (200, 'new-renderer')
after lag and resnapshot: (200, 'new-renderer')
after removal: no endpoint
```

## 接口与边界

- `Directory.replace(service, endpoints)` 替换一个服务的完整端点元组，顺序由发布方决定；
  空元组撤销服务；相同值不产生事件，返回 `None`，其余成功变更返回从 0 开始的序号。
  服务名由小写 ASCII 字母、数字和连字符组成，首字符为字母，最长 63 字符。
  `Endpoint(host, port)` 接受 ASCII 主机名/地址文本和 1–65535 的整数端口；
  不做 DNS 查询或连通性验证。重复端点、非法值及超出事件字节预算均被拒绝，不改变状态。
- `Directory.watch()` 在同一切点获取完整快照和下一条增量的游标。
  `View.resolve(service)` 返回该视图最后已应用位置上的端点；未知或已撤销服务返回空元组。
  调用方显式推进视图，未推进的读取可能滞后。
- `View.advance(timeout=0)` 应用一条增量；发现覆盖缺口时不会跳过缺口续用旧视图，
  而是替换为当前完整快照并接上新游标。`Progress(next_sequence, resynced)` 区分两种路径。
  无事件时抛出 `TimeoutError`、不改变视图。目录关闭后可读完仍保留的增量；
  如果不能恢复缺口，或已读完，则推进时抛出 `Closed` 并终止该视图的查找。
- 目录支持同进程并发发布和打开视图，每份视图由一个消费者使用。全量快照的成本随服务数增长；
  `capacity` 只限制保留的增量条数，`max_payload` 限制单条增量字节数，默认 65536。
  不做跨进程同步、持久化、消息确认、重试或上游健康路由。
- **应急连接槽**：`Directory(..., slot_limits=None)` 用 `Endpoint → 槽数` 的映射给本目录
  配置固定容量（最多 64 个地址，每地址 1–8 个槽；省略即不配置）。槽按监听地址计数，
  两个服务发布同一 host/port 时共用同一组槽。未配置槽的地址仍可发布和 `resolve`，
  但不会被发放租约。
  `Directory.set_reserve(demand)` 登记或替换下一次应急需求（服务名 → 数量，最多 32 个服务、
  每项 1–8、总计最多 64；空映射撤销）。只有当前权威目录与空闲槽能一次满足整组需求时才接受，
  失败保留原计划。`Directory.take_reserve()` 按调用时的权威目录一次领走整组槽，成功即消费计划；
  失败不会发放部分槽，也不会丢掉计划（目录变更可让计划暂时无解，供给恢复后仍可兑现）。
  `View.acquire(service)` 按本视图该服务的顺序，借出第一个仍在权威目录下、有空槽、
  且借出后仍能满足整组预留的地址；视图滞后时不会从已撤下的旧地址借槽，也不会使用本视图
  尚未看到的地址。预留不绑定具体地址，只要整组仍可兑现即可借出；若目录已变到预留无解，
  普通借用被拒绝而不是静默清空预留。两个借用接口都返回 `Lease`，`entries` 为
  `(服务名, Endpoint)` 元组；`close()` 一次性归还全部槽且可重复调用，发布更新、视图或目录
  关闭都不会提前释放租约。非法输入抛 `ValueError`，槽不足或分配无解抛 `SlotUnavailable`，
  未登记计划时 `take_reserve()` 抛 `NoReserve`。仅做进程内分配，不连接后端、不等待槽。

`broadcast.py` 保留原有 `Broadcast`、`Subscription` 接口和测试，作为目录的内部增量机制：
每个订阅有独立游标，覆盖通过 `Lagged(expected, oldest)` 显式报告，底层允许
`skip_lost()`，目录视图则用权威快照重建而不是跳过后继续应用。
