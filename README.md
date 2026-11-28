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

## 停机交接

撤下一个监听地址只改变后续发布的服务列表：还没读到广播的调用方仍可能拿旧地址发请求，
已经开始的请求也可能没结束。可选的可停机交接让调用方保护正在使用的监听地址，
让运维确认旧监听者何时真的能停、何时能以同一 host/port 启用新监听者。不涉及连接槽、
拨号或关闭后端。

- **配置**：`Directory(..., drain_endpoints=())` 声明本目录管理的地址（最多 32 个不重复
  `Endpoint`）。默认空集，此时新接口对任何地址都抛 `ValueError`；未使用该功能时行为不变。
- **跟踪视图**：`Directory.watch(tracked=True)` 打开可参与交接的视图，最多同时 64 个，
  超出抛 `RouteLimit`；`watch()` 原行为不变。只有跟踪视图能用 `open_route`。
- **请求保护**：`View.open_route(service, endpoint)` 保护该视图当前切点中、该服务端点列表里
  的受管地址。它不会偷偷推进视图、换地址或使用视图没见到的发布。成功返回带
  `endpoint` 与 `close()` 的对象：发请求前取得，整个请求结束后释放；`close()` 可重复调用，
  视图推进或关闭不替持有者释放。每个目录同时最多持有 256 个，超出抛 `RouteLimit`
  且不影响已有保护。视图未跟踪、地址不在该视图当前路由、或该切点已失去发起资格时抛
  `RouteUnavailable`；非法参数、未受管地址抛 `ValueError`。
- **开始交接**：运维先用 `replace` 撤掉指向该地址的所有服务名，再 `Directory.begin_drain(endpoint)`。
  是否仍被发布按同一 `Endpoint` 在整个权威目录判断。仍被发布、或已有未结束交接时抛
  `DrainConflict`；成功后立即返回交接对象，不等任何视图或请求。
- **交接状态**：`Drain.state` 按 `pending → ready → finished` 单调变化，可以一开始就 `ready`。
  `ready` 才表示运维可以停止旧监听者：所有受保护请求都已释放，且任何视图都不能再拿旧发布
  开始新请求——包括它稍后才从广播环里读到的旧发布。`pending` 期间旧视图仍可短期发请求
  （可先阻止新的旧路由，但必须等仍有资格的旧视图走出旧切点，不能无限期等待）。
  所有在途请求释放、且所有仍可能用旧发布开始请求的跟踪视图都已关闭或推进到交接开始切点
  （或更晚）后，立即可见 `ready`，无需再发布、计时器或后台扫描；新开的、已越过旧切点的
  跟踪视图不阻碍交接。目录关闭后没有视图能发受保护请求，就绪只等在途请求。
- **结束交接**：`Drain.finish()` 在 `pending` 时抛 `DrainPending`，`ready` 时结束本次交接
  并允许后续重新发布；重复结束同一对象是无操作，不会解开下一次交接。
- **发布冲突与轮次**：从 `begin_drain` 成功到 `finish`，任何把该地址放回目录的 `replace`
  （哪怕换服务名、或本是合法的协议版本）都抛 `DrainConflict`，不占序号、不发事件、不改目录
  或兼容切点。以成功 `begin_drain` 时的权威切点隔开两轮：重新发布后只有看到新一轮发布的
  切点能用于新请求；交接前的视图即使仍 `resolve` 出完全相同的 host/port，或 `finish` 后
  回放此前保留的旧加入事件，也不能用它发请求。不探测真实监听进程身份。
- **关闭语义**：目录关闭后不能再打开跟踪视图或开始交接（抛 `Closed`）；目录或跟踪视图关闭后
  不能再开始新的受保护请求。已取得的保护仍可释放，已有交接仍能查询与结束。

新异常 `RouteUnavailable`、`RouteLimit`、`DrainConflict`、`DrainPending` 均从 `directory` 导入。
只用标准库，不引入跨进程持久化。

## 协议兼容的滚动变更

应用与后端往往不能一起升级（旧应用只说 v1、新后端只说 v2）：任一端单独换到不兼容版本
都会断流。`Directory.configure_protocol_rollout(catalog)` 在发布初始地址之后一次性启用管理。
`catalog` 为 `服务名 → {版本名: (有序 Endpoint 元组, 提供的协议名元组, {被依赖服务名: 接受的协议名元组})}`：

- 管理 1–8 个服务，每服务 1–4 个版本，每版本 1–4 个不同端点；同服务的版本靠有序端点元组
  唯一识别。协议名沿用服务名格式，每个元组 1–4 个不同名称。一个版本的全部端点都具备该版本
  声明的协议能力（只建模是否存在共同协议）。
- 一个服务的所有版本必须声明相同的依赖对象；依赖都必须在此 catalog 内、不能依赖自己、可成环，
  全 catalog 最多 16 条有向依赖。
- 一个切点有效，指每条依赖上使用方接受的协议与提供方版本提供的协议至少有一个交集。
  启用时所有受管服务的当前端点都必须对应合法版本，初始切点必须有效，且每个版本的事件字节数
  都在目录上限内。无效配置或重复启用抛 `ValueError` 且不启用任何部分；成功返回 `None`，
  不改地址、不发事件。启用后再修改传入的映射不影响已生效的规则。

启用后受管服务仍用 `Directory.replace(service, endpoints)` 上线：空元组或未知端点元组抛
`ValueError`；若已知版本会让当前权威切点不兼容则抛 `IncompatibleRollout`。兼容性检查与发布在
同一把锁内完成，是一次原子操作——被拒时既不换址、不占序号，也不发事件。成功与无变化保留原
返回值，视图照常应用。一次只能发布一个服务版本。未受管服务与未启用功能时行为完全不变；
目录关闭后新接口抛 `Closed`。

`Directory.plan_protocol_rollout(targets, bridges=None)` 从同一权威切点规划：
`targets` 是受管服务到目标版本名的映射，`bridges` 为其中服务到允许临时使用版本名元组的映射
（每项最多 4 个）。未列入 `targets` 的服务全程固定在规划起点，但其依赖仍约束每一步；其余服务
只能用起点、目标及明确许可的临时版本。返回最少替换次数的步骤元组，每一步是可直接交给
`replace` 的 `(服务名, Endpoint 元组)`，每次只改一个服务、且改后所有依赖仍兼容；并列最短任选，
已到目标时返回空元组。无可行序列抛 `NoRolloutPath`（即使最终组合本身兼容）。规划不发事件、
不改目录、不取得独占权；实际发布时每一步仍按当时的权威切点重新校验，旧计划无法绕过校验。
非法输入或未启用时抛 `ValueError`。

## 可校验的副本读取

`verified_fetcher.VerifiedFetcher(size, block_size, digests, *, scratch_dir)` 把一个不可变
清单绑定到一个读取器：`block_size` 为 1–65536 的整数，正文非空、最多 128 块，只有末块可
短于块大小；`digests` 是按块顺序排列的小写十六进制 SHA-256 元组，条数须等于块数。`size`
不接受布尔值；非法清单抛 `ValueError` 且不发请求。`scratch_dir` 是调用方提供的本地临时
目录；本实现逐块校验、不在内存累计整份素材，因此不需要暂存，`scratch_dir` 不会被写入。

`fetcher.fetch(view, service, open_stream, emit)` 沿 `view.resolve(service)` 的当前顺序读取，
每次调用只取一次列表、不推进视图。空列表表示没有可交付副本；超过 8 个地址抛 `ValueError`
且不开始读取。`open_stream(endpoint, offset)` 打开该地址从指定块边界到素材结尾的流，
`read(n)` 的 `n` 为正且不超过块大小，允许短读，`b''` 表示结束；打开或读取抛 `OSError`
表示传输未完成。每个候选地址最多打开一次，切换地址前关闭当前流。

只有与清单摘要相符的完整块才通过 `emit(block)` 追加给下游；末块还会确认流没有超出清单
长度。正常完成时，`emit` 恰好按顺序收到全部块各一次，`fetch` 返回已交付字节数。副本用尽
或没有可用副本时抛 `FetchIncomplete`，其 `bytes_written` 是已成功交给 `emit` 的字节数，
因此下游只会是空或一段完整且已验证的连续前缀；切换副本时从已交付块之后继续，不重复、不
跳过。`emit` 抛出的异常原样上报，既不换副本也不记作坏副本。

**内容故障**：仅当确实收到完整块但摘要不符、或确实读到清单结尾之外的字节时，读取器才为该
`Endpoint` 记录对当前清单的内容故障。短读后断开、提前结束、打开失败、下游拒收都不记。
同一读取器后续独立 `fetch` 会跳过仍在故障记录中的地址（即使视图仍把它排第一），且不为它
调用 `open_stream`；新建读取器不继承记录。记录最多保留 64 个不同地址，满时淘汰最早一条。
不做素材同步、目录发布管理或连接槽分配；目录、广播、预留槽、滚动协议与停机交接行为不变。

## 调用侧批量查询

`batch_client.BatchClient(service, views, limits, transport)` 在调用方一侧把多个调用方的小查询
合批到同一后端。一个实例只服务一个服务：`views` 是调用方 ID 到它自己持有的 `View` 的映射
（1–8 个调用方），`limits` 是 `Endpoint → 该地址最多同时承担的未结算查询条数`（1–8 个配置地址，
每条上限 1–8）。组件不推进任何视图，只在每次 `dispatch()` 时调用该调用方视图的
`View.resolve(service)` 取当前切点；一个地址必须同时出现在该调用方视图和 `limits` 中才会被使用。
实例最多保留 64 个尚未取走终态的请求，超出 `enqueue` 抛 `OverflowError`。方法由同一事件循环
串行调用，transport 回调不重入；仅使用标准库。

最小例子（视图仍由调用方自己从 `Directory.watch()` 取得并按需 `advance()`）：

```python
from directory import Directory, Endpoint
from batch_client import BatchClient, OutcomeUnknown

directory = Directory(capacity=8)
view_a = directory.watch()
view_b = directory.watch()
directory.replace("render", (Endpoint("10.0.0.1", 9000),
                             Endpoint("10.0.0.2", 9000)))

client = BatchClient(
    "render",
    {"a": view_a, "b": view_b},
    {Endpoint("10.0.0.1", 9000): 1, Endpoint("10.0.0.2", 9000): 2},
    transport,  # 提供 submit(batch_id, endpoint, items) 与 lookup(batch_id)
)

request_id = client.enqueue("a", "query string")   # 整数 ID，实例内不复用
client.dispatch()                                   # 合批并尝试发送
terminal = client.take(request_id)                  # ("ok", value) / ("cancelled", None) / None
```

- **入队与取消**：`enqueue(caller, payload)` 只接受字符串，未知调用方抛 `ValueError`。
  `cancel(request_id)` 首次成功撤销一个尚未交付终态的请求时返回 `True`；已取消、已完成或未知 ID
  返回 `False`。`take(request_id)` 取走终态后删除该请求记录，因此也释放这个调用方侧的收件名额；
  没有可取终态（含已取过、未知 ID）时返回 `None`。每个调用方严格按提交顺序执行，同一时间至多有
  一个未结算的后端查询。
- **排队取消 vs 在途取消**：取消仍在排队的请求会立即放行该调用方的下一项。取消一个已经尝试发出、
  但还无法确认后端是否接单的请求时，调用方可以立即 `take` 到 `("cancelled", None)`，但在确认未
  接单或该批结算之前，这条请求仍占用原地址的后端额度并挡住同一调用方的下一项。
- **调度**：`dispatch()` 先在剩余额度内为每个可执行调用方的队首规划地址并发送，再推进未决批次的
  恢复。同一视图和额度下，只要计划中的批次都被接单，本轮实际发出的不同请求数达到本轮可发送的
  最大值；同一地址本轮至多一个新批次，选中该地址的查询合在这一个批次里。只剩一个可选端点的请求
  优先于可以改投其他端点的请求，后者不会挤掉前者。未接单的请求保留原提交次序等下轮；组件不自动
  推进视图，也不给已接单的请求改路由。
- **接单未知与额度释放**：`transport.submit` 返回 `True`/`False` 表示已接单/明确未接单；模块导出的
  `OutcomeUnknown` 表示无法确定。`transport.lookup(batch_id)` 返回 `True`/`False`/`None`。未决批次在
  查明前保留整批后端额度和原调用方顺序，但不影响与之无冲突的查询发送。transport 对同一批次 ID 与
  完全一致的地址和 items 支持幂等重送：恢复时沿用原批次 ID、地址和 items，返回既有结论且不第二次
  执行；仍无法确定时可再抛 `OutcomeUnknown`。**确认未接单后才释放该地址额度**，未取消的请求按原顺序
  重新调度；已接单则继续等结算，结算时才释放额度。
- **结算**：`settle(batch_id, values)` 处理整批结算，`values` 是以请求 ID 为键的字符串结果映射，顺序
  可变但必须恰好包含这批的全部原成员（含已取消项）。有效结算返回 `True`，并能直接确认一个尚未查清
  的批次已接单；未知或已结算 ID、成员缺失或多出、值不是字符串都返回 `False` 且不改任何状态。重复或
  迟到的旧批次结算不会释放新批次额度，也不会投递给新请求。每个终态至多交付一次。
