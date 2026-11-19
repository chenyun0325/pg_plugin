# 库存同步方案复现验证记录

2026 年 11 月 18 日 12 时 22 分，Asia/Shanghai。隔离实验的 22 项检查全部通过。结果文件为 [validated-report.json](validated-report.json)，包含每项结果、关键源码 SHA256 和工具版本；该报告对应修正 numeric 到 integer 转换后的版本。

## 环境与命令

| 项目 | 值 |
| --- | --- |
| 操作系统 | Windows amd64 |
| PostgreSQL | 18.7 |
| Go | 1.27.4 |
| PostgreSQL 端口 | 127.0.0.1:56489 |
| 数据库 | inv_review_mirror、inv_review_merged、inv_review_rollback |
| 实验数据和日志 | 仓库下 `tmp/inventory-review/b11ba544623648a586ec33f9e0723cc8` |
| 执行后状态 | 实验 PostgreSQL 已停止，临时 R 盘符别名已解除，数据保留 |

```powershell
# 使用仓库已有的完整 PG18 便携服务端；其他环境通过 -PgBin 指定目录。
pwsh -NoProfile -File ./inventory-sync/review/Run-ReviewLab.ps1

# 另行执行 pgcat 单元测试和静态检查。数据库集成测试由上面的隔离脚本执行。
Push-Location ./pgcat-yaml
try {
  $env:PGCAT_TEST_DSN = $null
  $env:PGCAT_TEST_ADMIN_DSN = $null
  go test -buildvcs=false ./...
  go vet ./...
} finally {
  Pop-Location
}
```

本次 `go test -buildvcs=false ./...` 返回 `ok pgcat-yaml`，`go vet ./...` 返回 0。新增 `TestInt32Exact` 覆盖固定小数位整数、int4 上下界、分数拒绝、溢出拒绝、NULL 和参数数量；无配置 DSN 时原有数据库集成用例会跳过，不能将这条单元测试命令视为真 WAL 测试证据。隔离脚本单独对实验库运行了 `TestPostgresWALBidirectional`。

## 已确认行为

| 验证内容 | 实测结果 |
| --- | --- |
| 正向初始快照 | 三表 10、3、2 合成 summary，时间取最大值，outbox 同步一致 |
| 反向初始快照 | 生成三张旧结构表，数量为 10、3、2，lock id 统一改为 summary id |
| 延迟校验 | 同事务先降低现货再清零预留与暂扣，最终 2、0、0 成功 |
| Origin 增量 | 正向更新和删除作用于 merged，反向目标仍保留快照旧值 |
| 直接写现有 mirror | 只替换 schema 的反向初始化被无来源既有记录拒绝，mirror 原值保持 |
| 本地反向写入 | summary 本地写入 20、4、2 后三路目标一致 |
| 目标失败 | 注入 lock 表 CHECK 后，三表仍为 20、4、2；checkpoint 保持 `0/2699AA0` |
| 恢复 | 移除注入约束后，原 YAML 与 WAL 重试成功，三表为 21、5、3；再次启动无重复行 |
| 本地兼容校验 | 21.5 的已确认现货数量被拒绝，summary 仍为 21 |
| 最终不合法库存 | 现货 1、锁定 2 的事务回滚，stage、summary、outbox 都无残留 |
| TBA | summary 可有数量 1.5 的 TBA 行，但 outbox 和旧结构目标不生成对应行 |
| 删除 | 本地删除已确认 summary 后三张目标表对应行全部删除 |
| 诊断 SQL | 由 stage 推导的阶段执行 Check-Merged.sql，问题结果集均为零行 |
| 原有回归 | projection smoke 和 pgcat 双向真 WAL 用例通过 |

## 本次必要修正

发现原反向 YAML 直接将 `numeric(24,6)` 字段写到 integer；实际传输文本为 `20.000000`，经 text 转 integer 时失败。新增 [inventory_functions.go](../../pgcat-yaml/inventory_functions.go) 中的 `int32_exact`，并将 [反向 YAML](../pgcat/summary-to-rollback-1ton.yaml) 的三种 quantity 映射改为使用该函数。转换先验证小数部分全为零，再进行 int4 范围检查，不舍入。

Windows 实验脚本使用临时 ASCII 盘符路径以兼容 initdb，并通过独立进程句柄等待 pg_ctl，避免后台 PostgreSQL 继承捕获管道后阻塞脚本。上述适配只用于实验编排，不改变库存计算逻辑。

## 验证边界

用户确认的目标场景是按 uid 单一业务主写，并通过共享 PG 分表回写 MySQL 以支持故障切换。上述记录是已有独立 rollback 实验的历史结果，不代表已验证 uid 混合主写、共享 mirror 身份接管、切换屏障或 PG 到 MySQL 的回写与回声过滤。目标方案和追加验收见 [技术评审第 7 节及第 8.4 节](TECHNICAL_REVIEW.md)。

复现从 SQL 模拟的 mirror 事务开始，没有连接实际 MySQL、提交 Flink 作业或执行 PostgreSQL 到 MySQL 回写。自定义 Flink PG sink 跨表事务和故障恢复、跨机器网络、生产容量、物理服务器崩溃、旧明细 id 引用恢复，以及持续完整 rollback 的级联策略，都需要各自的上线验收。

本次没有将反向任务指向活动业务 mirror；无来源覆盖测试仅使用本次实验的隔离镜像表。没有修改已部署数据库或运行中的正式任务。
