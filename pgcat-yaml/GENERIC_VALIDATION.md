# 通用映射分支验证记录

验证日期为 2026 年 11 月 18 日，Asia/Shanghai。实现提交为 `28ac543`，分支为 `codex/generic-mapping-uid-failover`，原分支 `codex/sidecar-identity-map` 仍指向 `9c87403`。

## 纯净源码验证

从实现提交使用 `git archive HEAD pgcat-yaml` 导出模块，在独立目录和端口 56490 执行新复现脚本。20 项顶层测试通过，0 项跳过；包含全部原分支 columns、sidecar、双数据库和 catalog 用例，并执行了 vet、构建和生产规则转换。PostgreSQL 为 18.7，Go 为 1.27.4，Windows amd64。

测试结果和源码 SHA256 保存在 [generic-validated-report.json](generic-validated-report.json)。纯净导出不包含工作区原有的未提交库存文件，证明新分支没有依赖这些文件才能构建或复现。

报告 SHA256 对应实际导出文件的字节；本机 Git 导出使用 CRLF，而部分工作区 Go 文件使用 LF，比较时应先核对换行格式，不能把换行差异判断为逻辑变更。

本机原始实验目录为：

```text
tmp/generic-clean-770c04068b0044a9b61cff675357c402/
  tmp/generic-mapping-lab/5be43f126ae043009f7bde88316893c6/
```

其中 `commands.log` 保存逐项测试与 WAL 日志，`postgres.log` 保存服务端日志，`wms-columns.yaml` 保存脱敏规则转换结果。实验服务已停止，临时盘符已解除，数据和日志保留。

原工作区另行执行同一脚本时 23 项顶层测试通过，0 项跳过；多出的 3 项是此前未提交的库存配置与整数转换测试。该次报告位于 `tmp/generic-mapping-lab/20352945c3284845942fa7fbcf8ee7b3`，不与纯净分支的 20 项混算。

## 验证内容

| 范围 | 已确认结果 |
| --- | --- |
| 原 columns 模式 | 快照、真实 WAL、TOAST、大整数、分片搬迁、垂直 patch、删除、防环、重启及提交后确认失败恢复通过 |
| 原 sidecar 模式 | 无业务来源列、历史身份与代次、主键变化、内部 outbox 中断恢复、旧观察保护及身份冲突回滚通过 |
| 原双数据库用例 | 真正独立两个 database 的 copy、严格 seed、回写、身份登记重试与静止通过 |
| 直接 1 比 1 | 同名目标模板的正向与反向修改通过 |
| 横向合表 | 两张分表相同 id 保持独立目标行，无业务 outbox |
| 原表回写 | 按 owner-table 白名单和 owner-pk 恢复原表原键，未覆盖同 id 的另一分片 |
| JSON 与删除 | 修改派生字段正确 patch 原 JSON，保留无关 key 和大整数；DELETE 只删除正确来源行 |
| 生产规则导入 | 实际生产文件 route/transform 的脱敏夹具、JSON、无 AS 别名、负向分片排除、动态表名及 ALL_MATCH 扇出通过 |
| 门控 | SQL 适配器拒绝错误 writer；Go 应用拒绝 draining、旧 epoch 与旧主写侧 |
| 切换屏障 | 未达到本地 checkpoint 的激活被拒绝，符合条件的新代次可继续应用 |
| 事务边界 | 源事务包含被拒绝事件时，其他目标行和 checkpoint 一起回滚 |

另对用户指定的原始 `job-yaml/mysql-to-sr-mutitables-v3-prod-merge-udf-addfield-mysql8-k8s-opt-fix2-opt2.yaml` 执行了实际 CLI 转换，结果为仓库 `output/generic-mapping/wms-columns.yaml`。输出不包含 hostname、username、password 连接字段。未连接该 YAML 中的生产服务。

## 复现

从仓库根目录执行：

```powershell
pwsh -NoProfile -File ./pgcat-yaml/scripts/Run-GenericLab.ps1
# 完整 PostgreSQL 服务端不在自动识别目录时：
# pwsh -NoProfile -File ./pgcat-yaml/scripts/Run-GenericLab.ps1 -PgBin C:/PostgreSQL/18/bin
```

脚本不读取部署 DSN，每次新建本地实验实例；数据库测试若跳过会判为失败。源代码不同可能导致测试总数不同，应核对报告的 source_hashes 和实际逐项结果。

## 验证边界

当前结果证明 PG 映射引擎及门控契约的实现和旧能力兼容。未执行实际 MySQL/Flink 端到端业务切换、真实 Java UDF 的语义对比、生产 DDL 和容量验收，未承诺跨库原子写权交接或零数据丢失故障切换。

共享 mirror 的 Flink 入站与 PG 到 MySQL 回写仍需接入同一事件代次及门控契约；如果连接器没有提供代次，本版本不会按当前 uid 状态伪造。完整使用条件见 [GENERIC_MAPPING.md](GENERIC_MAPPING.md)。
