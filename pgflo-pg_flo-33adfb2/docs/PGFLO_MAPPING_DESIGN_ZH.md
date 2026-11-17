# pg_flo 成熟度评估、改造设计与代码索引

## 1. 结论

基线 `pgflo-pg_flo-33adfb2` 适合继续改造成 PostgreSQL CDC/转换平台，但原版不能直接满足本项目：原 Router 是 `map[源表]单路由`，一条输入只能返回一条消息；规则只内置 regex/mask；Sink INSERT 使用 `ON CONFLICT DO NOTHING`；worker 转换失败仍 ACK；没有完整的 N:1 来源身份和 PostgreSQL replication-origin 闭环。

本目录代码已经完成单目标 PostgreSQL 范围内的核心闭环：

| 能力 | 实现 | 关键文件 |
|---|---|---|
| YAML 统一配置 | `pgflo/v1 ReplicationJob`，Strict YAML、环境变量、validate/preview/run | `pkg/jobconfig/config.go`、`cmd/job.go` |
| 表/字段映射 | schema.table 精确/正则匹配、显式目标投影 | `pkg/mapping/config.go`、`engine.go` |
| 1:N 水平分片 | 多 route + modulo，before/after 集合差分，分片键变更 DELETE+INSERT | `pkg/mapping/engine.go` |
| 1:N 垂直拆分 | 同一输入命中多个不同目标投影 | `pkg/mapping/engine.go` |
| N:1 合表 | 来源隔离身份、来源表跟踪、明确冲突策略 | `pkg/mapping/engine.go`、`pkg/sinks/postgres.go` |
| JSON/自定义转换 | `json_get/to_int64/to_string/lower/upper/concat_fields`，可注册 Go Function | `pkg/mapping/engine.go` |
| 源事务原子派生 | 一个源事务一个 `CDCEnvelope`；全部派生写入一个目标事务后 ACK | `pkg/utils/cdc_message.go`、replicator、worker |
| 双向防环 | Sink session/xact origin + pgoutput `origin=none` | replicator、Postgres Sink、双向样例 |
| 失败安全 | 转换/写入失败 NAK 并停止有序流；checkpoint 在 ACK 前 | `pkg/worker/worker.go` |
| 回放确定性 | envelope 携带 mapping version；版本不一致停流 | replicator、mapping engine |
| Commit/ACK 幂等 | 目标事务内 `pgflo_meta.applied_transactions` ledger | `pkg/sinks/postgres.go` |

这不是 MySQL source 的实现。用户提供的 Flink CDC YAML 是需求输入和配置风格参考；pg_flo 基线只解析 PostgreSQL pgoutput。MySQL 分库到 PostgreSQL 18 应保留 Flink CDC/Debezium source，或单独实现 MySQL binlog adapter 后再复用本 Mapping Engine。

## 2. 组件调用链

```text
PostgreSQL pgoutput(origin=none)
  -> BaseReplicator 按 BEGIN/COMMIT 缓冲
  -> CDCEnvelope{txid, commit_lsn, mapping_version, messages[]}
  -> NATS JetStream（一个源事务一个消息）
  -> Worker：旧 rules/router（兼容） -> Mapping Engine（0..N）
  -> Postgres Sink：origin + 全部目标 DML + origin_xact_setup + COMMIT
  -> 保存 consumer checkpoint
  -> ACK JetStream
```

Mapping Engine 的输出仍是扩展后的 `CDCMessage`，新增 typed `Values/OldValues`，因此转换新字段不需要伪造 PostgreSQL OID/tuple。旧 tuple/COPY 路径保持兼容。

## 3. 1:N 的三种语义

### 3.1 扇出

扇出是“一条变更复制到多个目标数据库”。当前 Mapping Engine 能生成多个目标表 mutation，但 Postgres Sink 只有一个连接，因此只实现同库多表扇出。跨数据库扇出不在本期：它需要 transactional outbox、每目标独立确认、补偿和可观测的部分成功状态，不能伪装成一个 ACID 事务。

### 3.2 水平分片

每个 shard 是一个 route。谓词分别作用于 before 和 after：

| before 命中 | after 命中 | 输出 |
|---|---|---|
| 是 | 是且身份相同 | UPDATE（Sink 以幂等 upsert 应用） |
| 是 | 否 | DELETE 旧 shard |
| 否 | 是 | INSERT 新 shard |
| 是 | 另一个 shard | 旧 shard DELETE + 新 shard INSERT |

示例按 `tenant_id % 2` 路由。负数余数已规范化。涉及谓词或旧目标键的表必须 `REPLICA IDENTITY FULL`；缺旧镜像或 unchanged TOAST 会报错停流，不会猜测或误删。

### 3.3 垂直拆分

多个 route 使用相同 source、不同 target，并显式列出字段。一个源 envelope 派生的主表/详情表 mutation 在同一个目标事务提交。当前不实现反向自动 JOIN：详情表 DELETE 是否删除主实体必须由反向业务 route 明确表达。

## 4. N:1 合表契约

### 4.1 默认：来源隔离

`identity.strategy: source` 自动加入：

```text
_pgflo_source       稳定的逻辑数据源 ID
_pgflo_source_table schema.table
_pgflo_source_pk    按 source-key 顺序编码的 JSON 数组
```

目标主键必须是三列组合。两个源表都有 `id=1` 时仍是两条记录；DELETE 只按这三列删除本来源记录。表重命名前需要迁移 `_pgflo_source_table` 或引入稳定 table ID，不能直接改 YAML 后假设身份不变。

### 4.2 业务键合并

`identity.strategy: mapped` + `target-key` 表示多个来源竞争同一业务键。必须显式选择：

- `conflict: upsert`：后到的有序事件覆盖该投影的全部非键列；
- `conflict: ignore`：已有键不修改；
- `conflict: error`：让唯一冲突回滚、NAK 并停流。

同时必须声明 `delete-policy: delete|ignore`。`delete` 只适用于能证明一个业务键始终只有一个来源 owner 的场景（双向样例的水平分片满足）；`ignore` 表示源 DELETE 不删除共享目标，但需要业务侧另行回收。默认的 `strategy: source` 使用精确来源键删除。

这三种都不是“多来源字段贡献合并”。如果 A/B 同时拥有一条业务实体的不同字段，应增加 contribution 表和字段所有权归并器；某来源 DELETE 只撤销自己的 contribution，剩余贡献重新计算。当前代码不会用机器时间或不同来源 LSN 武断比较冲突。

## 5. 自定义转换

YAML 可组合纯函数：

```yaml
outbound_id:
  source: payload
  transforms:
    - {function: json_get, args: [outbound_id]}
    - {function: to_int64}
```

新增业务函数通过 Go 注册：

```go
mapper, _ := mapping.NewEngine(job.Mapping)
_ = mapper.Register("warehouse_code", func(
    ctx mapping.TransformContext, value any, args []string,
) (any, error) {
    // 必须确定性、无网络/时间/随机数/外部副作用；不要修改 ctx.Row。
    return fmt.Sprintf("%s-%v", args[0], value), nil
})
```

生产中函数名和 `mapping.version` 应随代码发布冻结。旧 JetStream 事件带 mapping version，worker 配置不匹配会停流，避免用新函数静默重解释旧事件。JSON 使用 `json.Decoder.UseNumber`，不会把大整数强制变成 float64；路径不存在返回错误，SQL NULL 传播为 nil。

## 6. 双向回环

完整防环不是 `session_replication_role=replica`：

1. 目标预创建 replication origin；
2. Sink 在专用连接调用 `pg_replication_origin_session_setup`；
3. 在同一个目标事务提交前调用 `pg_replication_origin_xact_setup(commit_lsn, timestamp)`；
4. 捕获端 pgoutput 使用 `origin 'none'`，只发送没有 origin 的本地事务。

PostgreSQL 18 官方文档明确说明 `origin=none` 可用于避免复制节点间无限循环，replication origin 函数用于标记 session/transaction 的回放来源：

- https://www.postgresql.org/docs/18/protocol-logical-replication.html
- https://www.postgresql.org/docs/18/functions-admin.html

双向样例见 `examples/mapping/pgflo-job.yaml` 与 `pgflo-job-reverse.yaml`。

## 7. 事务、ACK 与恢复

- Replicator 只在收到 COMMIT 后发布完整 envelope；发布成功后才推进源端 NATS 持久状态。
- Worker 不会在转换失败时 ACK。当前源顺序流停止，防止越过毒消息造成因果反转。
- 一个 envelope 的所有水平/垂直 mutation 不会部分提交。worker 可把多个相邻 envelope 合并到一个更大的目标事务，但仍保持顺序。
- Sink 在业务 DML 之前、同一目标事务内登记 `(source_id, transaction_id)`；目标 COMMIT 后、ACK 前进程崩溃会由 ledger 跳过完整事务。这也让 `conflict:error` 的正常重投与真正业务键冲突可区分。
- JetStream checkpoint 在 ACK 前保存；`max-age: 0s` 避免原版默认 24 小时到期丢数据。

## 8. 当前生产边界与后续优先级

| 优先级 | 边界 | 建议 |
|---|---|---|
| P0 | NATS 单消息受 server `max_payload` 限制，大事务 envelope 尚未分块 | 增加 chunk+manifest+checksum；完整收齐后才 apply/ACK |
| P0 | unchanged TOAST 没有前态账本 | 建立与目标事务一致提交的 source-row state；当前选择停流而非错误写入 |
| P0 | 初始 copy 使用基线的导出快照实现，未做断点分块清单 | 增加 snapshot run/table/range ledger、barrier 与 resume |
| P1 | 只有一个 Postgres Sink | 跨库扇出使用 outbox/每目标进度，不承诺跨库 ACID |
| P1 | DDL 不做多目标 schema 变换 | 映射表生产建议 schema 由迁移工具管理，`track-ddl: false` |
| P1 | mapping 热更新不支持 | 新 version 建新 group，完成积压/切换后下线旧 group |
| P1 | N:1 contribution/字段所有权未实现 | 业务确认共享实体语义后单独建模 |
| P2 | 仅 modulo/equals 谓词 | 按需求增加 hash/range/list，并要求确定性与旧镜像 |

因此成熟度判断是：代码已足够进行单库目标的功能 PoC、集成测试和定向压测；在完成 P0（大事务分块、TOAST 状态、可恢复初始快照）以及 HA/监控/故障演练前，不应把它标成无条件生产成熟。

## 9. 代码与测试入口

- `pkg/mapping/engine_test.go`：水平迁移、垂直投影、JSON、大整数、N:1 来源隔离、删除、回环抑制、自定义函数、错误配置。
- `pkg/worker/wire_test.go`：事务 envelope 与旧消息兼容。
- `pkg/replicator/tests/base_replicator_test.go`：更新为一次发布完整事务。
- `examples/mapping/README.zh-CN.md`：编译、preview、真实 PostgreSQL 18/NATS、正反向验证命令。
