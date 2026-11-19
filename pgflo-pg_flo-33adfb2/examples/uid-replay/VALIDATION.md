# 本分支实测结果

验证日期：2026-11-19（Asia/Shanghai）。分支：`codex/pgflo-uid-failover`。

环境：Windows amd64，Go 1.27.4，PostgreSQL 18 容器，MySQL 8.0.44，NATS 2.11，JDK 21，Flink 1.20.1 本地执行入口，Docker Engine 29.9.3 / Compose 5.7.0。仅使用本次新建的 `pgflo-uid-lab` 实验环境。

最终二进制：`pgflo-pg_flo-33adfb2/bin/pg_flo.exe`。
SHA256：`D32C3D22B7056E93F9067128B45EDE75CFF68F0CE394E6A727ADA012196B880A`。

## 执行结果

| 检查 | 结果 |
| --- | --- |
| `go test -count=1 ./...` | PASS；包含原有测试、库存 WMS 场景、配置绑定和编译 UDF 注册测试 |
| `go vet ./...` | PASS |
| `go build -buildvcs=false -o bin/pg_flo.exe .` | PASS |
| forward / reverse YAML validate、JSON preview | PASS；精确原主键与部分 JSON patch 均在计划中 |
| `TestReplaySinkPostgres` | PASS；真实 PG 事务合表、分片、垂直拆分、删除归属、payload 哈希、回滚、旧 epoch 与屏障校验 |
| `TestReplayWALSnapshotToastRestart` | PASS；真实 WAL、导出快照、64 KB unchanged TOAST、JSON patch、kill/restart、离线 WAL、重复 envelope 不重写业务 |
| 位点与历史损坏 | PASS；丢失 NATS 捕获状态、删除 JetStream 消息、丢失 PG slot 均拒绝跳过历史 |
| 四跳 `Run-Lab.ps1 -PgFlo ... -FlinkClasspathFile ...` | PASS；最终 `tmp/pgflo-fourhop-final/result.txt` 为 PASS |
| 文档静态 JSON fixture 手工闭环 | PASS；json-source.sql + json-target.sql + json-reverse.yaml 实际运行 |

## 四跳闭环具体断言

1. UID 1 在 MYSQL/1 一次事务更新四张原表；合表为 `101,102,103,104`；回声不产生新业务标记。
2. 屏障收据到达两跳后切到 PG/2；旧 MySQL writer 被拒；PG 合表变更反向到四张原表和 MySQL。
3. 原表 ID 保持 `1,1,3,4`，stock 与 reserved 的相同 ID 不碰撞。UID 2 在 UID 1 归 PG 时继续由 MySQL 写，DELETE/INSERT 正确。
4. 反向捕获进程重启后，PG/2 切回 MYSQL/3；只修改 stock 不会把其余三项覆盖回切换前的值。
5. MySQL bridge 目标第二表约束失败，第一表、marker 和收据一起回滚；修复后从持久 offset 提交一次。
6. 恢复旧 offset 后，同事务再次交付被去重，后续事务继续处理。
7. pg_flo 合表目标第二项约束失败，整个目标事务回滚，进程停止。之后再排队一笔事务，修复并重启后依次应用两笔，没有越过失败记录。
8. 再完成 MYSQL/3 → PG/4 → MYSQL/5。重复执行同一个切换屏障返回成功，merged 保存四次切换历史，UID 2 值保持不变。
9. 混合 UID 源事务导致作业停止，目标两个 UID 都无部分变更；其他三条链路仍健康。
10. MySQL offset 文件缺失、截断时拒绝启动。

## 静态 JSON 样例结果

```text
id               = 9007199254740993
uid              = 0
customer_name    = Alice
stock.reserved   = 7
stock.available  = 30
unmanaged.code   = 9007199254740993
barcode          = keep-barcode
```

原主键精度、来源表定位、json_get、自定义 trim_space 和字段局部所有权均通过实际 job 验证。测试结束停止了启动的同步进程，保留实验库供查看。

## 原始记录与结论范围

本工作区原始记录位于 `output/pgflo-uid-replay/20261119/`：`go-test.txt`、`postgres-wal.txt`、`four-hop.txt`、`json-demo.txt`、`json-preview.json` 和二进制哈希。四跳每个进程的日志及 offset 在 `tmp/pgflo-fourhop-final/`。这些是本机测试产物，不是克隆源码后必需的依赖。

以上证明样例和已覆盖类型/路由的正确性，不替代任意生产表的上线验收。当前主键实测以整数和文本/复合字段为主，numeric、时间类型主键及自定义 PG 类型需要补充规范化与快照恢复测试。生产仍需落实应用写门禁权限、一致业务基线和持久存储。自动故障判主、跨库扇出/全局事务、反向新建或删除聚合业务规则、超大事务拆包、在线元数据清理不在本次通过结论内。
