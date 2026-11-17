# 验证记录

日期：2026-11-16，时区 Asia/Shanghai。

## 实际执行

- 编译工具链：go1.27.4 windows/amd64；模块最低声明 Go1.24。
- 运行数据库：用户提供的 PG18.6 测试实例，经既有 SSH 通道访问 mapping_poc。
- 无数据库单元测试 6 项通过：YAML 路由与分片差分、缺 key / NULL、来源身份保持、详情删除、JSON patch 保留其他字段且不修改源镜像、导入用户 WMS 配置。
- go vet ./... 通过。
- Windows amd64 编译通过；Linux amd64 CGO_ENABLED=0 交叉编译通过。Linux 可执行文件未在 Linux 主机实际启动。
- WMS 配置转换：从 job-yaml/mysql-shards-to-postgres18-wms-fix.yaml 生成 61 条路由到 examples/wms-pg-mirror.yaml；不复制凭证，不访问生产地址。

## 真实 WAL 闭环

TestPostgresWALBidirectional 最后完整运行通过，用时约 211 秒。

测试在 mapping_poc 内创建随机前缀 pgy_dmwgnykhhvb4 的 schema、publication、slot、origin：两个 schema 模拟两端。通过后清理测试对象；共用 _pgcat_yaml 元数据结构保留。

已断言：

1. EXPORT_SNAPSHOT 初始全量复制 2 条记录，反向仅 seed 源镜像。
2. JSON outbound_id=9007199254740993 精度保持。
3. 实际源 INSERT、UPDATE 的 pgoutput 二进制 WAL 被读取并应用。
4. 大 JSON / unchanged TOAST 更新依赖持久前态恢复，正确应用。
5. tenant_id 从偶数变奇数：旧分表记录删除，新分表写入，详情投影写入。
6. 跨分表时保留 customer.tier 和 keep 等未映射 JSON key。
7. 详情本地修改反向 patch 合表 JSON；详情删除仅清空归属字段，合表实体保留。
8. 合表 DELETE 清除相应分表及详情，不删除其他记录。
9. 同一个 id 出现在不同来源分表，目标保留两个独立身份。
10. 连续轮询最终无新业务变更，origin 事务不循环转发。
11. 关闭并重建 runtime 连接后从持久进度恢复，不重复记录。
12. 目标约束故障：目标数据和 checkpoint 不提交；移除约束后重试成功。
13. 模拟目标 COMMIT 后、源 slot advance 前中断；重试从 checkpoint 跳过已应用事务，不重复写入。

终端关键结果：

```text
PASS actual PG WAL: snapshot, JSON int64, TOAST, CDC, shard relocation,
vertical patch/delete, provenance, loop quiescence, restart,
rollback and post-commit replay (pgy_dmwgnykhhvb4)
--- PASS: TestPostgresWALBidirectional (211.26s)
```

此后对 JSON patch 的输入不可变性增加了单元断言，并通过对应测试；增加了 slot 所属数据库检查。源目录检查另行在 PG18 进行真实验证：TestPostgresCatalogGuard 约 5.3 秒通过，接受正确主键、拒绝非唯一键、publication 行过滤和列裁剪。

## 尚未声称验证

- 没有在生产 WMS 表上运行，实际业务 DDL 仍需提供 / 核对。
- 没有在两个物理 PG 实例之间做网络故障 / 主从故障切换验证；当前真实闭环为同 database 不同 schema。
- 没有在远端安装 pgcat-pgxs C 插件；验证的是默认 pgoutput + Go origin gate。
- 没有验证并行全量、大规模吞吐、无界大事务、跨 database 目标原子性、MySQL 外层回环。
- 不支持无写权仲裁的同时双主修改同一业务实体。

这些限制及故障处置已写入 README，不能将样例通过等同于生产全场景验收。
