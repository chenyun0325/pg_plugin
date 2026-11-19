# 库存与 WMS 出库追加测试

2026-11-18 的实际版本、命令、通过结果及清理状态见 [VALIDATION.md](VALIDATION.md)。

来源为仓库 `inventory-sync` 库存案例与 `job-yaml/mysql-to-sr-mutitables-v3-prod-merge-udf-addfield-mysql8-k8s-opt-fix2-opt2.yaml`。引用聊天的 `read_thread` 工具当前不可用，因此采用这些已落地的 YAML、SQL、技术评审和验证记录。

这里把库存案例已经生成的 summary outbox 接到 pg_flo，验证一行拆三表；库存三张旧表 JOIN、仓库字典、TBA、uid 写权切换仍由原库存方案负责。出库样例提取生产 YAML 的业务规则，以本机 PostgreSQL 替代 MySQL/StarRocks 进行测试。测试不连接生产地址。

## 测试矩阵

| 场景 | 样例/断言 | 覆盖层次 |
|---|---|---|
| 库存垂直拆分 | 20、4、2 同时写入现货/锁定/暂扣三表 | pg_flo 单元 + 真 WAL；pgcat 单元 |
| uid=0、负数量 | owner_id=0；-2、-1、0 不被过滤 | pg_flo 单元 + 真 WAL；pgcat 单元 |
| 固定小数位与 int4 边界 | 20.000000 精确变为 20；小数/溢出拒绝、NULL 保留 | pg_flo 单元 + 真 WAL；pgcat 单元 |
| 业务键迁移 | WH-A→WH-B，三张旧键删除、新键写入 | pg_flo 单元 + 真 WAL；pgcat 单元 |
| N:1 当前/历史/供应商/THD | outbound_id=7 共存，wh_type 分别 1/1/2/3 | pg_flo 单元 + 真 WAL；pgcat 单元 |
| 来源精确删除 | 只删当前表来源，历史/供应商/THD 保留 | pg_flo 单元 + 真 WAL |
| 复合序列号键 | 相同 outbound_id/upc 的 SN-A、SN-B 共存、精确删除 | pg_flo 单元 + 真 WAL；pgcat 单元 |
| delete 关键字/逻辑删除键 | delete=0→1，旧身份删除、新身份写入，无重复行 | pg_flo 单元 + 真 WAL；pgcat 单元 |
| JSON | 9007199254740993 保持精度；NULL 落库；缺 key/非法 JSON/溢出报错 | pg_flo 单元；正确值和 NULL 为真 WAL |
| Origin 防环 | 带 replication origin 的事务不输出，紧随其后的本地写入正常 | pg_flo 单元 + 真 WAL |
| 失败与恢复 | 第二张目标表注入 CHECK 失败，三表与 ledger 全部回滚，移除后原任务恢复 | pg_flo 真 WAL |
| 重复投递 | 重发已提交 envelope 不执行 DML；用目标哨兵值 99 验证 | pg_flo 真 WAL |
| 最终删除 | DELETE outbox 行清理三个目标投影 | pg_flo 真 WAL |

`source_table_key` 生成 `表名_id` 业务显示键；实际唯一键使用 source/source_table/source_pk 三列，复合源键包括 upc、bo_sn、tracking_number、delete，不只依赖拼接文本。

## 一键执行（PowerShell 7）

Docker Desktop 已安装后，新开终端会读取安装程序添加的 PATH。旧终端可先执行：

```powershell
$env:PATH = "$env:LOCALAPPDATA\Programs\DockerDesktop\resources\bin;$env:PATH"
docker version
docker compose version

# 在仓库根目录执行。包含单元测试、pgcat 对照、vet、构建、Compose 和真 WAL。
pwsh -NoProfile -File ./pgflo-pg_flo-33adfb2/examples/inventory-wms/Run-Scenarios.ps1

# 验证后停止三个测试容器；保留安装的 Docker Desktop。
pwsh -NoProfile -File ./pgflo-pg_flo-33adfb2/examples/inventory-wms/Run-Scenarios.ps1 -StopAfter

# 无 Docker 时只验证映射、CLI 和 pgcat 对照。
pwsh -NoProfile -File ./pgflo-pg_flo-33adfb2/examples/inventory-wms/Run-Scenarios.ps1 -MappingOnly
```

脚本优先使用仓库 `tmp/toolchain/go/bin/go.exe`，其他 Go 可通过 `-Go` 指定。结构化结果位于 `output/pgflo-inventory-wms/<时间>/report.json`，逐测试日志为同目录 JSONL。

Compose 仅绑定 127.0.0.1：源 PG 55442、目标 PG 55443、NATS 4232。E2E 每次创建随机命名的两张独立数据库、origin、slot 和 JetStream 资源，结束后清理自己的资源；不使用已有部署 DSN。

## 手动查看配置和事件转换

```powershell
cd ./pgflo-pg_flo-33adfb2
./bin/pg_flo.exe job validate --job examples/inventory-wms/job.yaml
./bin/pg_flo.exe job preview --job examples/inventory-wms/job.yaml --events examples/inventory-wms/events.yaml
```

10 个样例事件预计生成 18 条目标 mutation。需要持续运行演示链路时：

```powershell
docker compose -f examples/inventory-wms/compose.yaml up -d --wait
./bin/pg_flo.exe job run --job examples/inventory-wms/job.yaml
```

任务为 stream 模式，应在启动后再向源表 INSERT。源表已设置 REPLICA IDENTITY FULL。

## 本次实测发现并修复

1. PG numeric 解码结果是 `pgtype.Numeric`，原本按 Go 结构体文本解析会报错；新 `int32_exact` 使用其精确十进制表示，不经 float64、不舍入。新增 `source_table_key` 与 `warehouse_type` 支持出库样例。
2. 目标批量写入失败时，旧 worker 退出后会让消息留在 25 分钟 AckWait；现在退出前对缓冲和已预取未处理消息 NAK，测试可在数秒内重试原事件。

本测试没有增加 MySQL binlog/StarRocks sink，也不将 uid=0 的值测试解释为按 uid 主写权切换已实现。后者继续以 `inventory-sync/review/TECHNICAL_REVIEW.md` 的验收边界为准。
