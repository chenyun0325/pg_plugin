# pg-flo / pgcat 定制开发评估与参考实现

后续新增：工作区 `../pgcat-yaml/` 已提供独立 YAML 驱动的真实 PG WAL 运行程序，含快照、持久镜像、实际来源防环、分表迁移、双向样例及验证记录。本文以下为最初两组件评估与算法参考，原先“未接入上游”的说明针对 evaluation 目录，不代表新程序仍只有模型测试。

本目录保留早期的 pg-flo/pgcat 共享算法评估模型。后续已经把 pg-flo 路线接入实际 WAL→NATS→Worker→Postgres 调用链；集成代码、统一 YAML、测试与 PostgreSQL 18 闭环样例见 `../pgflo-pg_flo-33adfb2/docs/PGFLO_MAPPING_DESIGN_ZH.md` 和 `../pgflo-pg_flo-33adfb2/examples/mapping/README.zh-CN.md`。`PgcatAdapter` 仍只是 pgcat 路线参考，未接入 pgcat 上游循环。

## 1. 评估版本与证据

| 组件 | 固定版本 | 评估依据 |
|---|---|---|
| pg-flo | 用户本地 `../pgflo-pg_flo-33adfb2/` | 实际读取 router、rules、replicator、worker、sink、NATS、测试与示例。目录名与指定短 SHA 一致；未通过 git 元数据验证目录全部文件与原提交逐字节相同 |
| pgcat | `4e9c9c92d6525a54a6bf52df79ed75332ac43707` | GitHub master 查询结果，提交日期 2024-09-26；下载源码在 `../reference/kingluo-pgcat-4e9c9c9/` |
| pgcat-pgxs | `bd8d5975680f6870965240dc9cf1218229d9c3a9` | GitHub master 查询结果，提交日期 2024-09-24；下载源码在 `../reference/kingluo-pgcat-pgxs-bd8d597/` |

上游：[pg-flo 固定提交](https://github.com/pgflo/pg_flo/tree/33adfb2473093c9eb023fc56b5dd2fe97dadfbc3)、[pgcat 固定提交](https://github.com/kingluo/pgcat/tree/4e9c9c92d6525a54a6bf52df79ed75332ac43707)、[pgcat-pgxs 固定提交](https://github.com/kingluo/pgcat-pgxs/tree/bd8d5975680f6870965240dc9cf1218229d9c3a9)。

本评估针对这些版本。成熟度指满足本项目需求的工程基础，未通过星数、README 的 production-ready 宣称或版本号直接判定生产成熟。

## 2. 能力与成熟度判断

| 需求 / 维度 | pg-flo 指定版本 | pgcat 指定版本 |
|---|---|---|
| 表映射 | 有；Router 用源表裸名作 key，单一 TableRoute；同名跨 schema 隔离需改 | 有；精确 / 正则，按优先顺序选中一个目标 |
| 字段重命名 | 有；同时修改复制键名称；浅复制 ReplicationKey.Columns 会共享底层切片，需要修复 | 当前按相同列名绑定 RemoteIdx，需新增字段规则 |
| 自定义转换 | 有 Rule 接口和函数闭包基础；配置工厂内置仅 regex / mask，不具备本需求的通用 JSON 抽取 / 类型输出 | 无通用转换注册接口，需新增 |
| 初始同步映射 | COPY 结果以 CDCMessage.CopyData 进入 NATS，Worker 可统一应用规则；不是完全独立的映射通路 | 直接 COPY；复杂转换不能原样透传，需解码分批并走统一 Planner |
| 水平分片 | 没有完整的前后路由集合差分及迁移处理 | 只有表名映射，没有按行分片迁移 |
| 垂直拆分 | 一个输入返回一个 CDCMessage，需改为多条目标 Mutation | 一个源关系对应一个目标，需多目标计划 |
| N:1 | 多条配置可指向同表，但无来源隔离键 / 删除归属语义 | 有多源表名映射，仍需补来源隔离键、删除与冲突规则 |
| 双向防环 | 检查范围内没有完整的 origin 设置与过滤实现；不能直接上线双向 | C 回调明确过滤 origin_id != 0，apply.go 设置 origin，并将 DML / 进度同事务提交；仍需当前 PG 版本实测 |
| 事务边界 | Replicator 在 COMMIT 后逐行发布；下游消息不携带足够的源事务封装，Worker 按任意批次提交 | 源 BEGIN/COMMIT 对应目标事务，较适合保留同库垂直拆分的原子性 |
| 消息处理失败 | Worker processMessages 对 processMessage 错误仍 ACK，存在丢失风险 | 失败重启 / 重试；新增转换必须失败回滚，不能沿用跳过冲突覆盖所有情况 |
| INSERT 冲突 | ON CONFLICT DO NOTHING，会隐藏业务冲突；显式 NULL 还会被 INSERT 跳过 | 有可选 LWW，但不能自动解决跨源身份及字段归属问题 |
| 缓存 | 路由 / 规则裸表名匹配需改；多目标必须深复制 | localTables 仅以目标表为 key，跨源列绑定可能串用，需改复合 key |
| 队列 / 运维 | 已有 JetStream、规则、测试、示例；NATS MaxAge 默认 24h，离线超过保留期有丢消息风险 | 结构更直接、组件较少；Go 1.12 声明、老驱动和自定义 C 扩展增加现代化维护成本 |

判断：pg-flo 更适合作为带队列的可扩展 CDC / 转换平台；pgcat 的现有来源过滤和事务应用更贴近“自建 PG 双向同步”。两者对本次完整需求都需要实质性开发，均不应按现成完整产品验收。

若主要是自建 PG、同库合分表、双向同步，优先评审 pgcat 路线；若更看重队列解耦、规则扩展与持续扩展多类 Sink，优先评审 pg-flo 路线，同时把事务封装、ACK 和防环列为第一阶段必修项。扇出多库暂缓。

## 3. 共同的数据语义

### 3.1 N:1 身份、冲突、删除

第一版默认采用“来源隔离的行归并”，不把相同业务号自动合成一个实体：

```text
目标唯一键 = (_sync_source, _sync_table, _sync_pk)
_sync_source = 稳定的逻辑来源 ID，主从切换 / 服务重启保持不变
_sync_table  = 稳定来源表标识；示例使用 schema.table，正式版表重命名需要 ID 映射
_sync_pk     = 按固定类型和顺序编码的源复合主键
```

`source` 与 `epoch` 是捕获流身份；表上的来源列是记录身份；PostgreSQL replication origin 是复制应用事务标记。三者用途不同。反向捕获本地业务修改时，保留已有记录来源，不将当前捕获节点当成记录的原始来源。示例的 BeforeOwner/AfterOwner 提供这一接口，正式适配器需从受保护的来源列恢复。

默认冲突规则：单个业务实体 / UID 只有一个活跃写入端，按 writer_epoch 防止过期写端继续写。业务切换时暂停写入、整条链路追平、递增写权 epoch、开放新端。复制进度的 source epoch 与业务写权 epoch 应分别存储。数据库约束或写入网关必须检查写权；参考 SQLStore 尚未实现写权仲裁。

同一来源事务重投由 applied 表幂等；同一来源已提交流严格有序。禁止用两个来源的 LSN 或机器时钟直接比较新旧。来源隔离删除只删除同一复合身份的行，不能按裸 id 删除；多源合表禁止直接 TRUNCATE。

如果业务真正需要多个来源共享 `(tenant_id,order_no)` 一条记录，应另建 `contribution(source,source_pk,business_key,payload,version,deleted)` 贡献表，函数按字段归属 / 优先级归并。删除表示撤销该来源贡献，剩余贡献重新计算；所有贡献删除后才删除实体。该模式及可比较业务版本需要单独确认，本次参考代码未实现自动 JOIN / 聚合合并。

### 3.2 水平分片与垂直拆分

路由使用源字段的规范化值，分别计算 before、after，字段转换在具体投影中执行。若业务指定按转换后的值路由，配置必须明确该步骤与函数版本。

| 旧路由 | 新路由 | 目标行为 |
|---|---|---|
| 有 | 无 | 删除旧目标记录 |
| 无 | 有 | 插入新目标完整记录 |
| 相同目标和身份 | 相同目标和身份 | upsert 新投影；保留未归属该投影的目标字段 |
| 目标 / 身份改变 | 不同目标 / 身份 | 删除旧位置，再写新位置 |
| 无 | 无 | 记录无操作事务进度 |

第一版把水平分表、垂直表放在同一目标 database，DML、applied、progress 同事务提交。只支持固定且无重叠的分片规则，禁止映射两个投影到同一目标行。跨库水平迁移需要持久化 migration/outbox、目标确认与失败补偿，无法承诺一个 PG 本地事务，参考实现未提供跨库连接管理。

垂直反向写入必须使用字段所有权：主表负责实体存在和主字段；详情表只 patch 归属字段。删除详情记录不默认删除完整实体。缺少主记录时，明确选择拒绝 / 暂存 inbox，而非凭不完整字段创建实体。多表反向事件在一个源事务里合并后统一生成目标 patch。

### 3.3 自定义函数（重要）

代码接口 `Function func(value any, args []string) (any,error)`，通过 `Engine.Register(name,fn)` 注册受控 Go 函数。默认含 `json_get`、`text` 和 `json_put`；Column.Calls 可组合函数。Source="*" 可传入完整行的私有副本，实现跨字段函数。源字段与目标字段分离，允许改变类型和重命名；投影只包含需要写入的目标列。

```go
"customer_name": {
  Source: "payload",
  Calls: []Call{
    {Name: "json_get", Args: []string{"customer", "name"}},
    {Name: "text"},
  },
}
// {"customer":{"name":"Alice"}} -> customer_name = "Alice"
```

自定义函数例子见 engine_test.go 的 TestNullAndCustomFunction。生产注册使用函数名 + 不可变版本 / 代码摘要，跟随映射版本冻结在事务边界。函数必须确定性、无网络 / 当前时间 / 随机数 / 外部副作用；错误回滚整事务且不 ACK。参考实现注册表应只在启动阶段修改，不提供并发热修改保证。

SQL NULL 用 nil；字段缺失 / 未变化 TOAST 不能替换为 nil。json_get 路径不存在默认报错；SQL NULL 或 JSON null 向下传递 nil。大整数使用 json.Number，不经 float64。示例支持 JSON 对象路径，数组索引和业务类型函数可继续注册。

逆向转换不是正向函数的自动逆函数。`customer_name -> payload.customer.name` 必须读目标旧 JSON 并 patch 指定路径，保留其他 key；SQL 示例是 schema.sql 的 sync_meta.patch_customer。Go 示例用 Source="*" 和 json_put(base_json,customer_name,customer,name)，已测试保留同层及其他 JSON key；base_json 必须由目标事务内锁定读或一致的前态提供，不能从异步过期查询中取得。金额与枚举转换需要成对定义，空值、缺 key、溢出和未知枚举分别配置错误策略。

正式转换接口建议增加 `TransformContext{source,table,before,after,phase,version}`，支持访问多个字段；复杂垂直反向合并返回受控 Patch，而非任意 SQL 字符串。涉及键 / 路由的函数必须同时应用旧、新镜像。

### 3.4 镜像、初始化与恢复

参考 Planner 接口要求完整且有类型的 before / after。适配器先识别 pgoutput 的 'n'、'u'、文本与二进制，再填入 Row。UPDATE 缺旧镜像不得回退到新镜像计算旧分片；DELETE 不得假设所有业务字段都存在。

源端设置 REPLICA IDENTITY FULL 能帮助取得旧镜像，但不应假设所有 TOAST 新值总在 WAL 中。正式版需持久化每条源记录的前态 / 路由账本，以补齐 unchanged TOAST 和旧分片。状态更新、DML、目标进度原子提交；无法补齐时停止，不查源端“当前行”替代历史版本。

初始化使用逻辑槽导出的相容快照及对应起始位置，保持快照会话到所有分块读完；全量行也经同一 Planner。快照分块使用独立任务身份和检查点，CDC 等待快照基线完成后按 commit LSN 追平，不能混用临时自增序号或把快照行当普通 WAL 提交。pg-flo 现有 pg_export_snapshot()+pg_current_wal_lsn() 需要与逻辑槽一致性点重新审计。

双向初始化指定唯一权威端，反向 copy_data=false，初始化期间约束业务写入。全量普通 SELECT 不会自动按 origin 过滤，需按权威来源 / 数据归属限制复制范围。source_epoch 改变只用于经核验的重建 / 时间线切换，不应借新 epoch 绕过旧事件幂等。

## 4. pg-flo 的详细接入设计

### 4.1 调用链

```text
pgoutput(local-origin-only)
 -> 解码与镜像补齐
 -> CommittedTransaction{source,epoch,txid,end_lsn,seq,version,changes[]}
 -> JetStream 持久化事务
 -> Worker 按 source/epoch 串行消费
 -> Planner(before/after)
 -> 目标 PG 事务：origin + DML + applied + progress
 -> 提交成功后 ACK
```

| 文件 | 必须改造 |
|---|---|
| pkg/utils/cdc_message.go | 保留原始消息不可变；新增 typed CellState 与事务封装，不用 OldTuple=nil 时回退 NewTuple 的行为推断旧路由 |
| pkg/replicator/base_replicator.go | BEGIN 记录 XID，按 COMMIT 封装完整事务；解析 origin；从已持久化事务的 EndLSN 更新反馈，禁止凭最近收到位置推进 |
| pkg/replicator/base_replicator.go 两处 PluginArgs（约 169、283 行），replication_connection.go | PG16+ pgoutput 请求 origin='none'；不是仅设置 session_replication_role；两处复制启动入口都需要覆盖 |
| pkg/replicator/copy_and_stream_replicator.go | 导出槽快照协议、分块身份、统一函数与路由版本、明确 snapshot/stream barrier |
| pkg/routing/router.go | map[source+schema+table][]Route，ApplyRouting 改为 []Mutation；before/after 集合差分，垂直显式投影 |
| pkg/rules/types.go、engine.go、rules.go | 类型化函数注册、纯函数输入副本、错误回传；保留旧规则的兼容配置转换 |
| pkg/worker/worker.go | 用整个源事务代替任意消息批次；错误不 ACK；停止当前有序流并告警，可进入持久隔离队列但不能越过失败事务应用后续事务 |
| pkg/sinks/postgres.go | origin 设置、真正 upsert、来源键、NULL、受保护键、参数化 SQL；目标 ledger 与业务数据同事务 |
| pkg/pgflonats/pgflonats.go | 移除默认 24h 到期丢弃未处理关键事务的行为；明确存储上限、复制、备份、积压告警与反压，避免丢数据后仍推进源 WAL |

参考调用（是接入点示意，不是对现有 Worker 已完成的补丁）：

```go
// HandleCommitMessage: serialize and publish one full transaction, or durable
// chunks + a commit manifest; worker cannot expose chunks until all are durable.
// msg.EndLSN and immutable mapping version go into that envelope.
// Worker, after decoding and image hydration:
err := (pgmapping.PgFloAdapter{Engine: engine, Store: store}).Consume(txn, msg.Ack)
if err != nil { /* leave unacked; stop/retry this ordered stream */ }
```

大事务超过 NATS 单消息限制时分块并带数量 / 校验和，所有分块及 COMMIT manifest 落盘后才向源确认；Worker 验证完整后才能提交。生产建议每条有序捕获流一个活跃消费执行者，租约 / fencing 防止双活。NATS sequence 不能充当跨重建稳定事件身份。

### 4.2 最小闭环

1. 在 A 中准备 app.orders(id,tenant_id,payload)，插入 tenant=2 的 JSON 订单。
2. A Replicator -> NATS -> 改造 Worker -> B legacy.orders_0 + app.order_detail；两表同事务。
3. UPDATE tenant=3，验证 orders_0 删除、orders_1 新增、详情更新，JSON name 抽到 customer_name。
4. DELETE 验证两个投影清理；重投同事务无副作用。
5. B 普通本地业务编辑 -> 反向链路回写 A；B 复制应用写入不应再向 A 转发。
6. kill Worker 于目标 commit 后 / ACK 前；重启不重复应用；转换错误不 ACK，不越过失败事务。

本次已提供步骤 2~4 的新接口 / 算法模型与数据库 Sink runner；步骤 1 的实际 CDC 采集、NATS 事务封装、步骤 5 的真实双向网络链路仍需按上表接入。origin 独立 SQL 验证脚本见下文。

## 5. pgcat 的详细接入设计

### 5.1 调用链与改造

```text
pgcat 输出插件（过滤 origin_id != 0）
 -> Go BEGIN/Relation/DML 解码、镜像补齐
 -> 同事务共享映射版本 -> Planner
 -> 同一目标 DB 多表 DML
 -> 目标进度与 origin_xact_setup -> COMMIT
 -> 按持久化进度反馈源槽
```

| 文件 / 部分 | 必须改造 |
|---|---|
| subscription.go | 加载 route、projection、function version；配置固定到源事务边界 |
| relation.go | SourceRelation -> []TargetPlan；按 (subscription,source relation,target,version) 缓存，不能只用目标表名；列名映射与输出类型验证 |
| dml.go | 替换单 rowData SQL 模板为 []Mutation；before/after 分别路由；键与参数分离；正确引用标识符 |
| apply.go | 保留事务及 origin 语义；同事务内应用所有目标投影；异常完整回滚；续传检查点不丢失 |
| tablesync.go | 纯重命名可分离 COPY 源 / 目标列；有 JSON 转换 / 拆分时改为分批解码 -> Planner -> staging / 受控 upsert |
| pgcat-pgxs SQL | 新配置表、配置备份导出、权限、通知函数和扩展升级脚本；不要仅修改 1.0 初始安装脚本 |
| pgcat-pgxs pgcat.c | 保留 origin filter；在选定 PG 版本重编译测试；不要以 session_replication_role 代替 origin |

建议新配置实体：route(subscription,route_id,version,source_schema,source_table,target_schema,target_table,predicate,enabled)、projection(route_id,version,target_column,source_column,function_name,args)、key_policy、field_ownership。函数版本单独注册，JSON 只保存受校验参数。新通知函数应读相应表的 subscription 字段；现有 pgcat_cfg_trigger 仅对 subscription / table_mapping 分支处理，不能直接挂到新表并假设有效。

参考接入：apply.go 的 BEGIN 创建事务封装；DML 解码保留原始字段并补齐镜像；COMMIT 调用 Planner 后，在现有 applyTx 内依次执行 Mutation，再按现有路径提交进度 / origin。若使用参考 SQLStore，它自行管理事务，因此必须替换现有事务管理，不可嵌套调用。生产实现更适合把 SQLStore 的“事务内执行”拆成独立函数，复用既有 pgcat 事务。

pgcat 同步 worker 原先的直接 COPY 不能绕过自定义函数。复杂映射的同步状态应按 route + 源表记录，失败时不把某个已完成投影当成整表全部完成。

### 5.2 最小闭环

两端安装 pgcat-pgxs。A 的两个 legacy 分表 -> B app.orders，B 的本地合表编辑 -> A 对应分表与详情表。先验证同源不同分表 id=1、不同源同 id=1 都不碰撞；DELETE 只影响其来源身份。双向分别独立订阅与 origin，只有本地变更继续传播。测试实际 pgcat 插件 origin 过滤，不能只做 Go 字符串标记测试。

两端表不一定有相同业务字段；正、反向 projection 分开配置。B JSON 合表 -> A 标量分表使用 json_get；A 标量 -> B JSON 使用字段 patch，并在目标同事务锁定实体，避免多个垂直表相互覆盖 JSON。

参考代码 ExampleEngine 中“legacy 分表 -> app.orders”与“app.orders -> legacy 分表 + detail”分别演示两种方向的算子，不构成一个可直接连线的业务双向模型（示例两侧列布局不同）。真实业务的成对字段 schema / 逆向 patch 必须按业务配置，不能把这两个演示方向直接互相订阅。

## 6. 已提供的代码与运行方式

| 文件 | 实现内容 |
|---|---|
| engine.go | 类型化完整镜像接口、来源身份、函数注册、JSON key 抽取、显式字段投影、水平路由差分、映射版本校验、两个事务适配入口 |
| sqlstore.go | database/sql 目标执行器；专用连接 origin；参数化 DML；同事务 applied、progress；串行连续序号检查 |
| example.go | N:1、JSON 提取、tenant 奇偶分表、独立 detail 投影的配置示例 |
| engine_test.go | 可自动运行的 10 组语义测试，包含失败回滚 / ACK 丢失重投模型和逆向 JSON patch |
| sql/schema.sql | 目标演示表、复合来源键、进度与 applied、逆向 JSON patch SQL 函数 |
| live/main.go | 使用真实 PG 的新接口闭环：轮流调用两个适配入口，验证跨分表迁移、JSON 提取、垂直拆分、删除与事务重投 |
| sql/verify_origin.sql | 实际逻辑槽过滤测试，可选原生 pgoutput 或 pgcat 插件 |

### 6.1 无数据库验证（本机已执行）

```powershell
cd evaluation
$env:GOCACHE = Join-Path (Get-Location) '..\tmp\gocache'
& '..\tmp\toolchain\go\bin\go.exe' test -v ./...
& '..\tmp\toolchain\go\bin\go.exe' vet ./...
```

portable Go 位于工作区 tmp/toolchain；从 Go 官方下载并核对公布 SHA256，未修改系统安装。普通环境可直接使用 go test / go vet。

### 6.2 真实 PostgreSQL 验证（尚未执行）

准备一个可丢弃的 PG16+ 测试 database，wal_level=logical。不要对业务库执行演示 schema；脚本会创建固定名称，重复运行应新建测试库，不自动删除既有对象。

```powershell
$env:PG_DSN = 'host=127.0.0.1 port=5432 dbname=mapping_poc user=postgres'
# 密码通过 PGPASSWORD / pgpass / Secret 提供，不写入仓库。
psql $env:PG_DSN -v ON_ERROR_STOP=1 -f evaluation/sql/schema.sql
cd evaluation/live
go mod tidy
go run -buildvcs=false .
# 预期 PASS；该 runner 是目标侧真实事务验证，未运行实际上游 WAL/NATS。
```

每次用全新的测试库运行 runner，避免演示事务 ID 与已存在 applied 记录混淆。

防环的数据库机制验证使用另一测试库（需要创建 publication、logical slot、origin 权限）：

```powershell
# pg-flo 拟采用的 pgoutput 路径
psql $env:PG_DSN -v pgcat_mode=false -f evaluation/sql/verify_origin.sql
# 目标实例已安装 pgcat 输出插件时，验证 pgcat 路径
psql $env:PG_DSN -v pgcat_mode=true -f evaluation/sql/verify_origin.sql
```

断言顺序：普通本地 INSERT 可见 -> 带 replication origin 的 INSERT 被过滤 -> reset 后新本地 INSERT 可见。这验证机制，不代替 A->B->A 全链路和初始化验收。只过滤所有远端来源的拓扑不会将 A 的数据经 B 自动转发到 C；多跳转发不在第一版范围。

## 7. 验收与开发阶段

阶段一：冻结来源 ID / 合并键、字段所有权、UID 写权及双向函数；跑通 JSON 抽取 / patch、水平分片迁移、垂直拆分、来源隔离删除的目标侧闭环。

阶段二：分别接入选定组件的实际 WAL 与初始化路径；pg-flo 先修事务封装、错误 ACK、队列保留和进度问题；pgcat 先修多源缓存、字段计划、COPY、SQL 与版本兼容。

阶段三：真实 A<->B 回环、初始权威端初始化、主键 / 分片键变化、SQL NULL / JSON null / 缺 key / 大 JSON TOAST、转换失败、目标 commit 后进程退出、复制连接重连、权限失败、槽积压恢复。对负载、故障和数据一致性记录结果后再进入灰度。

本机本次已完成核心 10 组测试与 go vet；真实 PG runner 也已用 pgx v5.7.5 编译及 go vet 通过（依赖校验和沿用用户本地 pg-flo 的 go.sum）。机器没有 PostgreSQL / Docker / psql，因此真实数据库运行、SQL origin 脚本、自定义 C 扩展兼容及两款上游实际端到端链路均未验收。参考实现也尚无全量快照协调、持久镜像账本、写权仲裁、动态规则迁移、跨库补偿和多源业务聚合模式。
