# 通用映射与 uid 主写切换

本分支从 `codex/sidecar-identity-map` 的 `9c87403` 创建，分支名为 `codex/generic-mapping-uid-failover`。保留原 columns 模式、sidecar 模式、静态路由、分片搬迁、JSON patch、事务 checkpoint 和内部身份恢复协议。新增能力通过显式配置开启，已有 YAML 不需要增加业务 outbox，也不需要为兼容新引擎改写。

验证记录见 [GENERIC_VALIDATION.md](GENERIC_VALIDATION.md)，包含从已提交纯净源码运行的新旧真 WAL 回归结果。

## 1 方案与已实现范围

| 场景 | 执行方式 | 业务 outbox |
| --- | --- | --- |
| 1 比 1复制和改名 | 直接读取源业务表 WAL，配置字段和表路由 | 不需要 |
| 多张分表汇入一张表且各源行独立保留 | 来源身份加目标唯一键，逐行映射 | 不需要 |
| 一个源行投影到同库多张表 | 多路 mutation 与 checkpoint 同事务 | 不需要 |
| 按原始来源反向恢复 | sidecar 身份加白名单定位原表及原键 | 不需要 |
| 三表组成同一库存行 | 保留已有库存 SQL 投影或接入专门状态算子 | 按业务发布适配需求选择 |
| 业务模型无法直接发布或需要独立事件契约 | 显式建立发布投影或消息 outbox | 可选 |

`_pgcat_yaml.identity_outbox` 是现有 sidecar 跨库身份登记的恢复协议，继续保留。它与逐业务表的 outbox 不同。来源旁路也不能补偿目标唯一键容量不足：异源记录使用相同 id 时，目标仍需真实可用的复合键或合成键。

本分支的 uid 功能是目标事务内的写权和事件代次校验，以及冻结和带本地 checkpoint 屏障的激活 API。它不是完整跨 MySQL、Flink、两个 PG 节点的自动切换控制器；各跳必须提供事件代次和应用进度。既有库存表没有事件代次字段时，不应直接打开门控并假设系统能识别历史归属。

## 2 路由能力

### 2.1 动态同名目标

```yaml
route:
  - name: copy_same_name
    source-table: 'legacy\.(customer|supplier)'
    sink-table: app.<>
    replace-symbol: <>
    source-key: [id]
    primary-keys: [id]
    include-all: true
```

`legacy.customer` 写入 `app.customer`，`legacy.supplier` 写入 `app.supplier`。启动时按 publication 的实际表枚举并检查目标；不会在读到业务事件时偷偷创建表。新增源表和 DDL 仍使用受控迁移，不支持自动 schema evolution。

### 2.2 排除规则与匹配模式

```yaml
route-mode: all-match
route:
  - name: non_sharded
    source-table: 'legacy\..*'
    exclude-source-tables:
      - 'legacy\..*(?:_[0-9]+|_[0-9]+_history)'
    sink-table: app.<>
    replace-symbol: <>
    source-key: [id]
    primary-keys: [id]
    include-all: true
```

默认 `all-match` 保留原来所有匹配规则均生效的行为；需要首条规则优先时显式使用 `first-match`。给定生产 YAML 中的 thd 等非分片源，可能同时命中合表和兜底同名表，转换后仍然扇出，不擅自取消其中一条路径。

### 2.3 按来源恢复原表

```yaml
route:
  - name: restore_original
    source-table: 'app\.orders'
    target-from: owner-table
    allowed-targets: [legacy.orders_0, legacy.orders_1]
    source-key: [record_key]
    primary-keys: [id]
    columns:
      id: {function: json_index, args: [{from: __owner_pk__}, {literal: 0}]}
      uid: {from: uid}
```

`target-from` 与 `sink-table` 互斥；必须提供有限目标白名单。目标来自原始 owner，不从 `_pk_id` 字符串里猜分片。已有源实体保留原表和主键。新系统本地新建的实体没有旧表 owner，必须配置独立的新行分片、ID 和身份策略；不会自动写入任意旧表。

原有取模分片、等值路由和 JSON patch 继续可用。若源实体从当前分片移往另一个分片，原始 owner 和当前物理位置仍按原 sidecar 协议区分。

## 3 投影与生产 YAML 导入

新增顶层 `transform` 保存结构化表达式，路由显式设置 `use-transform: true` 后生效。选择规则匹配具体物理表名，不再要求 route 正则字符串与 transform 正则字符串完全相同。一个表匹配多个 transform 时停止并要求消除歧义。

转换器支持当前生产文件需要的 `*`、字段、固定数字和字符串、`||` 拼接、函数嵌套、显式或省略 `AS` 的别名。`json(...)` 显式转换为 `json_get`，保留 JSON 大整数；其他函数必须已注册。不会执行任意 SQL 或加载生产文件中的 Java 类。若现有 Java JSON UDF 有额外默认值、类型或缺键语义，需要按实际实现核对，不能只凭函数名认定完全相同。

给定文件的非分片负向预查被转换成 include 加 exclude；其他不支持的 lookaround 直接报错。带 filter 的输入不会被静默丢弃，需明确改写为原生过滤规则。`replace-symbol` 当前支持 `<>`，不声称实现所有 Java 正则和替换语法。

在仓库根目录构建独立二进制，保留旧 bin 文件：

```powershell
Push-Location ./pgcat-yaml
try {
  New-Item -ItemType Directory -Force -Path ../output/generic-mapping | Out-Null
  go build -buildvcs=false -o ./bin/pgcat-yaml-generic.exe .
  ./bin/pgcat-yaml-generic.exe convert-wms `
    --input ../job-yaml/mysql-to-sr-mutitables-v3-prod-merge-udf-addfield-mysql8-k8s-opt-fix2-opt2.yaml `
    --output ../output/generic-mapping/wms-columns.yaml
} finally { Pop-Location }
```

导出只读取路由和投影，不复制连接地址、用户或密码。输出的 StoreServer 和 StoreServerHaV2 是原逻辑 schema 名，PG 部署时要按真实镜像和目标 DDL 校对，包括区分大小写的标识符。

sidecar 导入仍要求提供真实业务唯一键文件：

```powershell
./bin/pgcat-yaml-generic.exe convert-wms --input INPUT.yaml --output OUTPUT.yaml `
  --identity-mode sidecar --namespace wms_v2 --sink-id new_pg `
  --target-keys reviewed-business-keys.yaml
```

键文件按目标表名填写，动态目标用配置模板名（例如 `app.<>`）作为键。模板覆盖的物理目标必须具有相同的已评审键定义；不同结构应拆分路由。不能从 StarRocks 投影就断言原 PG 源表拥有非 NULL 唯一约束，运行前的 catalog 检查仍会验证。

## 4 uid 写权与代次

以下配置只适用于事件已带真实写入代次的链路：

```yaml
ownership:
  namespace: inventory_v2
  uid-field: uid
  epoch-field: write_epoch
  writer: mysql
```

反方向配置相同 namespace 和适合该表的 uid 字段、`writer: pg`。每个目标数据库保有自己的 `_pgcat_yaml.write_ownership`；部署控制器必须协调相关节点，不能把两库各自的登记当成全局原子租约。

应用时在目标事务中锁定 uid 登记并核对 writer、epoch、active 状态；锁持有至业务 DML 和 checkpoint 一起提交。源事务内任一事件不符则整体失败，checkpoint 不前进，不按行部分成功。不同 uid 可登记不同主写侧。跨 uid 业务事务需要整体写权一致，不能靠丢掉其中几行消除冲突。

初始化步骤：

1. 用新 job 和 slot 配置 epoch 门控，执行 `setup --config JOB.yaml`，创建元数据但不创建逻辑槽或执行快照。
2. 显式登记初始主写侧和代次，例如 `INSERT INTO _pgcat_yaml.write_ownership VALUES('inventory_v2','1','mysql',1,'active')`。
3. 执行 `init`，随后 `run`。缺少登记或事件代次直接报错。

切换 API 在目标数据库执行：

```sql
SELECT _pgcat_yaml.freeze_writer('inventory_v2','1',1);

-- 此前必须停止该 uid 的旧主业务写入，并逐跳确认旧主最后提交已落地。
-- 这里的 job 和 LSN 必须属于当前目标库 checkpoint 的真实源位置。
SELECT _pgcat_yaml.activate_writer(
 'inventory_v2','1',1,'pg',2,
 '{"forward_job_v2":"0/12345678"}'::jsonb
);
```

`freeze_writer` 使用预期 epoch 的 CAS；`activate_writer` 只接受 draining 状态、更大 epoch 和非空的已达到本地 checkpoint 屏障。业务侧停写、MySQL GTID 到 PG LSN 的逐跳确认、所有目标节点协调以及最后开放新主，仍由部署控制器完成。当前接口只验证调用者提交的本地屏障，不能证明未提供的外部链路已追平。

外部 PG sink 或业务写入适配器可以在同一事务调用同一个校验契约：

```sql
BEGIN;
SELECT _pgcat_yaml.assert_writer('inventory_v2','1','mysql',1);
-- 在此执行该 uid 的 INSERT UPDATE DELETE，随后 COMMIT。
COMMIT;
```

该函数与冻结操作使用同一行锁。若 Flink 自定义 sink 没有在每次目标写入中接入同等门控，pgcat 不能阻止上游 Flink 已经覆盖共享 mirror；所以共享 mirror 的端到端安全仍需要连接器适配。`assert_writer` 必须与 DML 同一事务，不能先单独调用后再另开事务写业务表。

当前 `epoch-field` 来自新行，DELETE 来自旧行。换代后删除旧 epoch 的行，需要先合法地更新到新 epoch 再删除，或另外实现能持久记录删除代次的事件信封。本版本不会把消费者当前看到的 epoch 冒充事件代次，也不会自动替原业务表增加该列。

## 5 可复现实验与兼容性

```powershell
# 仓库有便携 PG18 完整服务端时自动识别；否则指定自己的服务端目录。
pwsh -NoProfile -File ./pgcat-yaml/scripts/Run-GenericLab.ps1 `
  -PgBin 'C:/PostgreSQL/18/bin' -Port 56489
```

脚本不依赖库存 outbox 实验，建立随机目录的本地 PostgreSQL，运行所有单元、catalog、columns WAL、sidecar WAL、双数据库恢复和新增通用 WAL 测试。两个数据库 DSN 均指向该隔离实例，未执行的数据库用例会让脚本失败。然后执行 vet、构建和生产规则脱敏转换。报告、日志、二进制和转换结果保存在 `tmp/generic-mapping-lab/<随机目录>`；结束停止实例，Windows 临时盘符解除，数据不自动删除。

新增真 WAL 用例覆盖无业务 outbox 的 1 比 1复制、横向合表、重复 id 的分片隔离、动态表名、原表回写、JSON patch、大整数、删除和防环。uid 用例验证 draining 拒绝、过期 epoch 拒绝、未追平屏障拒绝、合法新代次及不合法混合事务回滚。原有 sidecar 身份 outbox 中断恢复等测试继续执行。

可手工查看 [样例 DDL](examples/generic/schema.sql)、[正向配置](examples/generic/a-to-b.yaml) 和 [反向配置](examples/generic/b-to-a.yaml)。只在可丢弃测试数据库执行 DDL。先正向 copy 再反向 require-binding seed；不能把目标原有数据无条件当成新实体接管。

## 6 与库存案例及 MySQL 链路的边界

库存原 SQL、触发器和可选业务 outbox 保留，没有强制所有表走库存路径。原始库存 YAML 继续使用原来的 columns 模式；新功能不会自动把它转换成带正确三表 lineage 的 sidecar 多源聚合。

当前新旧 pgcat 均只使用 PG 源和 PG 目标。已有 `job-yaml/pg-to-mysql-superuser.yaml` 可作为外部回写起点，但仍需适配真实表范围、事务性、uid 代次和回声过滤。PG 的 origin 不会自动穿过 MySQL binlog。MySQL/Flink 的真实端到端切换、突发故障 RPO 和生产容量不包含在这次本地 PG 测试中。

现有配置字节不变时，原 job 的 config hash 计算方式保持不变。开启新路由、门控或重写导出的 YAML 时，仍须新建版本化 job 和 slot，不能覆盖旧 checkpoint 混跑规则。
