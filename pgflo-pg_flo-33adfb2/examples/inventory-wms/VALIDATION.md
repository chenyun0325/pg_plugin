# 2026-11-18 库存/WMS 验证记录

时间：2026-11-18 16:09，Asia/Shanghai。完整一键脚本退出码 0。

| 项目 | 实测 |
|---|---|
| Windows | Windows 11 Pro 26100，WSL 3.0.5 |
| Docker Desktop | 4.99.0，当前用户安装 |
| Docker Engine/CLI | 29.9.3 |
| Docker Compose | v5.7.0 |
| PostgreSQL 容器 | 18.7（postgres:18） |
| NATS 容器 | 2.11.17（nats:2.11-alpine） |
| Go | 仓库便携工具链 1.27.4 |

Docker 安装包来自 `desktop.docker.com`，安装前检查 Authenticode 状态 Valid、签名者 Docker Inc。用户 PATH 已由安装程序添加 Docker bin。Engine 启动成功；Compose 实际拉取镜像并启动了两个健康的 PG 容器及 NATS。

执行命令（仓库根目录）：

```powershell
./pgflo-pg_flo-33adfb2/examples/inventory-wms/Run-Scenarios.ps1
```

通过结果：

- pg_flo 全量普通测试与 go vet 通过。
- pgcat 全量普通测试与 go vet 通过；已有需单独 DSN 的数据库用例未被此命令激活。
- 新增 pg_flo 映射 9 个子场景通过。
- 新增 pgcat 库存/WMS 对照 4 个子场景通过。
- pg_flo 真 WAL/NATS/目标 PG 的 8 个子场景通过，E2E 耗时约 17 秒。
- Job YAML Strict 校验通过，10 个 preview 事件产出 18 条 mutation。

真 WAL 测试具体覆盖：三表拆分与键迁移；四来源合表与精确删除；SN 复合键与 delete 字段；JSON 大整数和 NULL；origin 回放过滤后继续捕获本地写入；第二张目标表失败时三表和 ledger 回滚、原任务恢复；重发 envelope 不覆盖哨兵值；最终删除三个投影。

对应的结构化报告：`output/pgflo-inventory-wms/20261118-160826/report.json`；同目录保存 `pgflo-unit.jsonl`、`pgcat-inventory-wms.jsonl`、`pgflo-wal.jsonl`。

本次配置 SHA256：`C9B4B2F4CDFFC9F7C4C5F3912DCFEC694B36CDA3C13215148FE1BB0216BDA512`。

完成后执行本示例专属 `docker compose down --volumes`，清理了本次容器、网络及合成数据，包括早期中断测试留下的临时库；源代码和报告保留，Docker Desktop/Engine 保持可用。复现脚本会重新创建环境。

本记录不包含实际 MySQL/Flink/StarRocks 网络验收、按 uid 主写权切换或 PG 到 MySQL 回写；这些沿用库存技术评审中的独立验收要求。
