# pgcat-yaml：PG16+ / PG18 的 YAML 合分表复制程序

这是独立、可编译的 Go 复制程序，参考 kingluo/pgcat 的“逻辑解码 -> SQL 目标应用 -> 同事务进度 + replication origin”设计，参考 pg-flo 的规则 / 路由分层，重新实现 YAML 配置、全量、真实 WAL、持久镜像和变更路由。不是在旧版 Go 包上只增加一层 Adapter，也不是声称已向上游合并的补丁。旧版源码仍保留在工作区 reference/。

## 1. 本版实现范围

- 1:1 表名、字段名映射，显式投影、默认同名 include-all、exclude。
- N:1：稳定来源 ID + 原始来源表 + JSON 编码源复合键；可配置目标业务 / 组合键；不同来源的已有目标键冲突立即失败。
- 1:N：水平取模 / 等值分片、垂直投影；旧、新镜像分别路由；跨分表删除旧位置、插入新位置；同一源事务的所有目标写入原子提交。
- JSON key 抽取、int64 转换、组合主键、来源常量、自定义 Go 函数、JSON 路径 patch。缺 key 默认报错，NULL 单独处理，大整数不经过 float64。
- 源逻辑槽 EXPORT_SNAPSHOT 初始化；同一相容快照扫描所有发布表；目标全量数据、源镜像账本、checkpoint 一个事务提交。
- 真实 pgoutput WAL 解码；持久 row_state 补齐 unchanged TOAST；源主键变化时删除旧身份、写入新身份。
- 双向：目标事务设置 replication origin；读取端识别 OriginMessage，带来源的事务只维护镜像和进度，不再向业务目标转发。两个方向可以长期运行。
- 失败时整个目标事务回滚，checkpoint 不推进；进程重启从持久进度继续；同 job / 同 slot 使用 advisory lock 防止同一数据库上的两个消费者。

本版不做：MySQL binlog 采集、多目标 database 扇出、跨库分片搬迁原子提交、任意 Flink SQL 执行、动态 DDL 复制、热改映射、多源字段聚合 JOIN、无仲裁的双主并发写。源码是完整执行路径，不代表已经完成生产容量认证；大规模灰度前须按实际数据和故障环境验收。

一个 YAML job 有一个源 database、一个目标 database，所有输出投影在同一目标库内。跨库双向使用两个 job。示例为了便于复现，使用你提供的 mapping_poc 的两个独立 schema 模拟两端；真实部署可以连接两个数据库。

## 2. 与上游的对应关系

参考固定版本：

- [kingluo/pgcat](https://github.com/kingluo/pgcat/tree/4e9c9c92d6525a54a6bf52df79ed75332ac43707)：apply.go、dml.go、relation.go、tablesync.go。
- [kingluo/pgcat-pgxs](https://github.com/kingluo/pgcat-pgxs/tree/bd8d5975680f6870965240dc9cf1218229d9c3a9)：pgcat.c 的 origin filter，以及扩展状态和函数。
- 用户本地 pgflo-pg_flo-33adfb2：routing、rules、worker、replicator、postgres sink；两个组件的源码成熟度评估见 ../evaluation/README.md。

本版默认原生 pgoutput，不需要安装 pgcat-pgxs，因此可直接验证 PG18。这里用 SQL `pg_logical_slot_peek_binary_changes` 读取 pgoutput 二进制事务，目标提交后才调用 `pg_replication_slot_advance` 确认。不会先调用 get_changes 消费槽再写目标。快照导出使用复制协议，正常增量使用 SQL 轮询。

默认 origin=any 是有意设计：读到远端事务后更新源行镜像，避免复制行后续本地修改时缺失 TOAST；这些事务绝不再向业务目标应用。不要把“读取远端镜像”误当作“转发远端事件”。这个机制在 runtime.go 的 OriginMessage / commit 中实现。

`source.plugin: pgcat` 可使用已安装的上游 pgcat 输出插件，其 C 回调在源端提前过滤远端事务；本次没有在远端安装该自定义 C 插件，不能把 pgoutput 测试结果当作插件兼容测试。pgcat 模式无法持续观察远端行镜像，若 FULL 旧行仍不足以补齐 TOAST，程序会停止并要求重新建立快照。PG18 推荐本版默认 pgoutput 路径。

## 3. 文件说明

| 文件 | 内容 |
|---|---|
| main.go / config.go | CLI、严格 YAML 校验、配置摘要、路由匹配 |
| functions.go / codec.go | 函数注册、结构化表达式、类型解码与无损数字 |
| engine.go | 来源身份、前后路由、水平分片、垂直投影、详情删除策略 |
| store.go | 目标键、来源冲突检查、JSON patch、参数化 SQL、镜像与进度 |
| runtime.go | publication 检查、导出快照、逻辑槽读取、origin、防重放、重启 |
| convert.go | 从指定 Flink WMS YAML 导出本程序的业务映射，不导出密码 |
| engine_test.go | 配置、路由、身份、JSON、WMS 导入测试 |
| integration_test.go | 在随机命名空间运行真实 PG WAL 双向验证，结束清理自身对象 |
| examples/ | 双向配置、DDL、样例数据、更新与查询、生成的 WMS 映射 |
| scripts/Run-Demo.ps1 | 双方向运行脚本，不保存密码 |
| deploy/ | Linux systemd 服务样例 |

## 4. YAML 与转换规则

完整样例见 examples/a-to-b.yaml、examples/b-to-a.yaml。

```yaml
version: 1
job: unique_job_v1
source:
  id: stable_logical_source
  dsn-env: PGCAT_SOURCE_DSN
  publication: my_publication
  slot: unique_slot_v1
  plugin: pgoutput
  snapshot: copy              # copy：初始化目标；seed：仅初始化源镜像
sink:
  dsn-env: PGCAT_TARGET_DSN
  origin: unique_apply_origin
route:
  - name: example
    source-table: 'legacy\.orders_[0-9]+'
    sink-table: app.orders
    source-key: [id]          # 源记录稳定唯一身份，可配置多列，必须非 NULL
    primary-keys: [_sync_source, _sync_table, _sync_pk]
    include-all: false
    columns:
      order_id: {from: id}
      _pk_id:
        function: concat
        args: [{from: __table_name__}, {literal: '_'}, {from: id}]
      outbound_id:
        function: json_int
        args: [{from: param}, {literal: outbound_id}]
```

source-table 为完整 schema.table 的正则，程序自动加全匹配边界；点应转义。支持逗号分隔的表规则列表，复杂正则不应包含逗号。相同源表的所有投影必须配置相同 source-key。include-all 会复制源列；未知目标列报错，可用 exclude 排除。未配置的目标列使用默认值 / NULL；必要 NOT NULL 列缺失由数据库拒绝。

表达式支持 from / literal / function + args 递归组合。支持元数据：__table_name__、__schema_name__、__source_id__、__owner_source__、__owner_table__、__owner_pk__。不能写入保留的 _sync_* 列，程序自动维护这些列。用户函数不得修改输入、调用外部服务、依赖当前时间或随机数。

内置函数：json_get、json_int、json_index、json_object、concat、int64、text、lower、if_eq。json_get 接收文档及多个路径组件；json_index 单独用于 JSON 数组。if_eq 是普通确定性函数，参数都会求值。新增函数可放在 custom_functions.go：

```go
package main
import("fmt";"strings")
func init() {
    Register("normalize_status", func(args []any)(any,error) {
        if len(args)!=1 { return nil,fmt.Errorf("normalize_status requires one arg") }
        if args[0]==nil { return nil,nil }
        switch strings.ToUpper(fmt.Sprint(args[0])) {
        case "PAID", "1": return "paid",nil
        case "UNPAID", "0": return "unpaid",nil
        default: return nil,fmt.Errorf("unknown status")
        }
    })
}
```

重新编译后 YAML 使用 `function: normalize_status`。程序不从 YAML 执行任意脚本。JSON 路径不存在报错；SQL NULL / JSON null 返回 nil；数组越界、类型转换溢出报错并回滚。

### JSON 反向 patch 与垂直表删除

```yaml
columns:
  id: {from: order_id}
  param: {from: payload}      # 仅在目标行不存在时作为 JSON patch 初始文档
json-patches:
  param:
    - path: [customer, name]
      value: {from: customer_name}
```

已有目标行的 JSON 在同一事务内 FOR UPDATE 读取，只改指定路径，其他 key 保留；跨分片新增位置的 JSON 使用 payload 初始化，再 patch。多级对象缺失会创建对象；中间路径是非对象时失败。不能从源的当前行查询历史镜像。

`on-delete` 默认 delete；详情向合表回写时使用 `patch-null`，只清空该投影的非键列和 JSON 路径；也可以 ignore。`require-existing: true` 禁止凭详情创建不完整合表记录。示例详情删除不删除整个订单。

### 合并键、冲突及写入归属

来源身份自动保存在 _sync_source、_sync_table、_sync_pk，第三列是源复合键的 JSON 数组编码。原始来源在反向和跨分表迁移后保持不变；当前捕获节点不替换原始来源。source.id 跨服务重启和主从切换必须保持稳定。

目标 primary-keys 必须对应实际唯一约束 / 主键且取值非 NULL。WMS 导出默认用三列来源键，保留原 _pk_id 作为业务字段；不会仅用裸 id 吞掉历史表或其他来源的数据。如果保留原业务组合主键，应核对跨来源碰撞规则后修改 DDL 与 YAML。

冲突策略：不同来源身份占用同一个目标键立即失败，整个事务回滚；同来源事件按源提交顺序执行。对于同一逻辑实体，两端业务必须单主写，切换前停止旧端写入并追平。这里没有声称实现任意双主并发冲突自动仲裁或按机器时钟 LWW。两边同时改同一个业务实体不在支持条件内，需业务写权协调。

对原始旧表上的无来源记录，反向覆盖默认拒绝。显式 `adopt-unmarked-source: legacy` 只允许来源 ID 为 legacy 且原始来源表等于当前目标表的记录绑定身份。不要对任意来源放开接管。示例对 new_app 在其原始合表内本地新增的记录做对称处理。

## 5. 编译：Windows PowerShell

工作目录：本项目 pgcat-yaml。已安装 Go 和 psql18，可直接执行。

```powershell
Set-Location 'C:\Users\杭州澜创科技\Documents\ChatGPT\pg_plugin开发\pgcat-yaml'
go version
psql --version
go env -w GOPROXY=https://goproxy.cn,direct
go mod tidy
go test -v ./...
go vet ./...
New-Item -ItemType Directory -Force bin | Out-Null
go build -buildvcs=false -o bin/pgcat-yaml.exe .
```

无 PGCAT_TEST_DSN 时，真实数据库测试会显示 SKIP；其他单元测试执行。保留 go.sum 校验，不设置 GOSUMDB=off。-buildvcs=false 用于本工作区的 Git 所有权 / stamping 限制，不影响复制逻辑。

## 6. 样例完整实施：当前测试服务器

前提：SSH 隧道 127.0.0.1:15432 已到 172.31.31.245:31129；库 mapping_poc、用户 testuser；服务端 PG18.6，wal_level=logical。若通道已断开，在工作区根目录执行 tools/Start-PgTunnel.ps1。密码交互输入，不保存文件。

下面 schema.sql、seed.sql 是样例数据操作；仅用于独立测试库。首次执行会创建 pgcat_demo_a / pgcat_demo_b，已有同名 schema 时失败，不自动覆盖。

```powershell
$env:PG_DSN = 'host=127.0.0.1 port=15432 dbname=mapping_poc user=testuser sslmode=disable'
$env:PGPASSWORD = [System.Net.NetworkCredential]::new('',(Read-Host '数据库密码' -AsSecureString)).Password
$env:PGCAT_A_DSN = $env:PG_DSN
$env:PGCAT_B_DSN = $env:PG_DSN
psql -X -v ON_ERROR_STOP=1 -d $env:PG_DSN -f examples/schema.sql
psql -X -v ON_ERROR_STOP=1 -d $env:PG_DSN -f examples/seed.sql
.\bin\pgcat-yaml.exe check --config examples/a-to-b.yaml
.\bin\pgcat-yaml.exe check --config examples/b-to-a.yaml
# 初始化期间禁止 B 业务写入；先 A 权威全量，再 B 仅 seed 镜像。
.\bin\pgcat-yaml.exe init --config examples/a-to-b.yaml
.\bin\pgcat-yaml.exe init --config examples/b-to-a.yaml
```

seed 数据为 orders_0 的 101/Alice/tenant2 和 orders_1 的 202/Bob/tenant3。101 的 outbound_id=9007199254740993，用于检查大整数精度；JSON 还有 customer.tier=gold、keep=preserve me，用于检查反向 patch 和跨分片迁移保留其他 key。

两个终端分别运行（脚本各自提示密码）：

```powershell
# 终端一，在 pgcat-yaml 目录
.\scripts\Run-Demo.ps1 -Direction A
# 终端二，在 pgcat-yaml 目录
.\scripts\Run-Demo.ps1 -Direction B
```

第三个终端设置 PG_DSN / PGPASSWORD 后执行：

```powershell
psql -X -v ON_ERROR_STOP=1 -d $env:PG_DSN -f examples/verify.sql
psql -X -v ON_ERROR_STOP=1 -d $env:PG_DSN -f examples/change-b.sql
# 等待两个消费者追平后查询
psql -X -v ON_ERROR_STOP=1 -d $env:PG_DSN -f examples/verify.sql
```

预期：101 从 A.orders_0 删除并出现在 A.orders_1；tenant=3、customer.name=Alice revised；customer.tier、keep 仍保留；A.order_detail 出现对应 note。B 合表记录原始来源仍为 A.orders_0，循环消费者最终不再产生业务变更。

继续验证垂直回写和删除（先确认对应 UID 写权切换，避免两端同时写）：

```sql
UPDATE pgcat_demo_a.order_detail SET note='detail local edit' WHERE order_id=101;
-- 等待：B.orders.note 与 B.orders.payload.note 更新，其他 JSON 不变。
DELETE FROM pgcat_demo_a.order_detail WHERE order_id=101;
-- 等待：B.orders 仍存在，note 及 payload.note 为 null。
DELETE FROM pgcat_demo_b.orders WHERE order_id=101;
-- 等待：A.orders_1 的 101 和其 detail 被删除，不影响 202。
```

Ctrl+C 停止 job 后，再用相同 YAML 执行 run，即从持久 checkpoint 续传，不重新 init。`--once` 只处理一个解码批次，适合手工排查；生产使用常驻 run。

## 7. 自动真实 WAL 验证

```powershell
$env:PGCAT_TEST_DSN = $env:PG_DSN
go test -run TestPostgres -v -count=1 -timeout=8m .
Remove-Item Env:PGPASSWORD
```

测试在当前 database 创建随机前缀 schema/publication/slot/origin，只使用自己创建的对象；结束后清理测试对象，保留共用 _pgcat_yaml 元数据表。不会使用旧的 app、legacy 或业务 store 表。验证快照、真实 WAL、JSON 大整数、TOAST、水平搬迁、垂直回写/删除、同 id 不同来源、防环收敛和重启。

## 8. 使用你提供的 WMS YAML

已经生成 examples/wms-pg-mirror.yaml。可重新生成：

```powershell
.\bin\pgcat-yaml.exe convert-wms `
  --input '..\job-yaml\mysql-shards-to-postgres18-wms-fix.yaml' `
  --output examples/wms-pg-mirror.yaml
```

导入内容：source-table / sink-table、所有路由分组、__table_name__+'_'+id/outbound_id 的 _pk_id、wh_type 分类、tracking_number/delete/upc/bo_sn 等源复合键、json_int(param,'outbound_id')。导出只读取 route/transform，不复制原文件中的账户密码，不连接 MySQL。

原始文件是 Flink MySQL 采集作业，本程序输入是 PG 镜像表。必须先由现有 Flink 链路把 MySQL 分表按原表保留到 PG 镜像层；若实际 PG 镜像表已经重命名，需要调整生成的 source-table。StoreServer 是区分大小写的 schema；PG 中须正确引用。源表应具备完整列结构并设置 REPLICA IDENTITY FULL。

实际 WMS 表 DDL 没有包含在该配置文件内，不能凭 YAML 推造全部业务列的类型。生成映射需要结合真实 DDL核对 source-key 与目标列；目标表须增加 _sync_source/_sync_table/_sync_pk text、对应唯一约束，并确认业务原主键是否还会造成来源间冲突。严禁为了通过测试直接删除业务约束；必要时建立独立同步接收表。

建独立 publication，只包含这次 job 的 PG 镜像表。所有发布表必须有匹配路由，否则 check 失败；不要复用全库 publication。设置两个 DSN 环境变量后执行 check -> init -> run。本次没有连接原 YAML 中的生产 MySQL / PG 地址，也没有对 store 业务表执行 DDL。

## 9. Linux / EC2 部署

Go 1.24+，源 PG16+，推荐 PG18。连接的源角色需 SELECT、逻辑解码/复制槽权限；目标角色需目标 DML、元数据 schema DDL、replication origin 函数权限。演示可用受控管理员；正式环境按目标 PG版本确认授权，不以 SUPERUSER 为普通运行前提假设。

```bash
cd pgcat-yaml
go mod download
go test ./...
go build -buildvcs=false -o bin/pgcat-yaml .
sudo useradd --system --no-create-home pgcat
sudo install -d -o pgcat -g pgcat /opt/pgcat-yaml /etc/pgcat-yaml
sudo install -m 0755 bin/pgcat-yaml /opt/pgcat-yaml/pgcat-yaml
sudo install -m 0644 deploy/pgcat-yaml@.service /etc/systemd/system/
# 将每个 job 的 YAML 放入 /etc/pgcat-yaml/a.yaml、b.yaml。
# 创建受限 a.env、b.env，包含相应 PGCAT_*_DSN 和 PGPASSWORD。
# 使用权限 0600 的文件或 Secret 注入，密码不写入仓库。
sudo systemctl daemon-reload
# 初始化按同样环境先手工执行 check/init；成功后才启用常驻服务。
sudo systemctl enable --now pgcat-yaml@a pgcat-yaml@b
sudo journalctl -u pgcat-yaml@a -f
```

如果源、目标使用不同密码，应在两个 DSN 中分别注入 password 或配置 PGPASSFILE；单一 PGPASSWORD 只适合两端相同密码。生产使用正确的 TLS verify-full / CA 配置；当前演示 sslmode=disable 是针对已有 SSH 通道及测试服务器 ssl=off。

PG 参数至少 wal_level=logical、足够的 max_replication_slots 和 max_wal_senders，并确认复制来源容量。初始化的 EXPORT_SNAPSHOT 用复制连接；常驻消费者需源 SQL连接、目标SQL连接。请监控 source 槽 retained WAL、checkpoint、错误日志和数据库磁盘。离线消费者的槽会保留 WAL，不应无人值守无限堆积。

## 10. 故障与变更

- 源 / 目标断连：当前事务回滚，进程返回失败；systemd 重启后从 target checkpoint 重放。checkpoint 与 row_state 和业务 DML 同事务，所以目标提交后、源槽确认前退出也不会重复产生业务效果。
- 全量期间退出：目标事务回滚，但源槽可能仍存在。确认对应 job 没有 checkpoint、没有其他消费者且允许重新初始化后，显式 `init --recreate-slot`。不得对已初始化 / 其他用途的槽使用此选项。
- YAML 修改：摘要不一致会拒绝沿用旧 checkpoint。新规则使用新 job/slot，规划数据回填与清理；不支持运行中改 YAML 后无条件续传。
- 源槽 ahead-of-target：可能发生目标恢复到旧备份，程序拒绝继续，要求重新建立一致快照。不能手工把 checkpoint 填大来掩盖丢失。
- 类型不兼容 / 缺 JSON key / 身份冲突 / TRUNCATE：失败停止，不静默跳过、不推进失败事务。修复历史事件处理方式或受控重建快照；仅在后续事务修好源行并不会消除之前失败的历史事务。
- 源新增发布表 / 结构变化：本版没有自动 schema evolution / 自动新表全量协调。先停止 job，评审目标 DDL及映射，使用受控版本化重建。
- 源表限制：启动时校验普通表、完整 publication 列集合、无 publication 行过滤、无生成列；source-key 需为非 NULL 的受支持标量，并有对应 UNIQUE / PRIMARY KEY。数组、时间戳等复杂源键需先设计稳定规范化编码。目标跨投影外键需按实际事务顺序设计，或使用合适的可延迟约束，本版不自动排序外键依赖。
- 全量是单快照、单目标大事务；初始化大库需要较长 MVCC 快照、WAL 和目标事务空间。增量 SQL peek 按批读取，但一条大事务仍会整体驻留内存。不承诺 pg-flo 式并行 bulk copy、无界大事务或既定吞吐量。
- 所有远端事务停止继续业务转发，适合直接 A<->B；不自动支持 A->B->C 级联。MySQL<->PG 外层回环仍需原 Flink 链路的版本/幂等规则，不能由这里的 PG origin 自动解决。

## 11. 验证记录

见 VALIDATION.md。只有明确记载的测试结果才视为已执行；生成的 WMS 映射不等于已连接生产库验证。
