# pg_flo：YAML 映射、按 UID 写主与多次切换

本分支在 pg_flo 的 WAL → NATS → Worker → PostgreSQL 路径上实现 UID 写权协议。PG 之间的正反向处理进程均为 **pg_flo**。MySQL binlog 捕获与 MySQL 落地继续使用工作区已有的事务适配器，Go 进程自身没有 MySQL connector。

| 需求 | 本分支行为 | 验证入口 |
| --- | --- | --- |
| 表名、字段映射 | YAML 指定 sources、target、columns；支持正则源表 | mapping 单元测试、库存场景 |
| 水平分片 | when.modulo / when.equals 路由；旧路由退出时删除旧投影 | mapping 生命周期测试、ReplaySink PG 测试 |
| 垂直拆分 | 同一源事务的多条 route 写入不同目标表，同一目标数据库内原子提交 | ReplaySink PG 测试、库存 WAL 测试 |
| N:1 合表 | source 身份三元组隔离同名 ID；也可显式映射复合主键；conflict 选 upsert / ignore / error；delete-policy 显式声明 | 合表碰撞、来源归属删除测试 |
| 双向回环 | 源事务标记 + writer/stage + PG origin 配对；回声不写业务、不产生下一跳标记，但提交收据和前态 | 四跳库存实测 |
| JSON / 自定义函数 | json_get 支持对象与数组路径；Go 编译扩展可由 YAML 引用；json-patches 按对象路径回写，保留其他字段 | 真 WAL TOAST 测试、json-reverse.yaml |
| 同 UID 一个写主、多次切换 | ACTIVE → DRAINING → 屏障两跳到达 → epoch+1；不同 UID 独立；旧代事务拒绝 | MYSQL1 → PG2 → MYSQL3 → PG4 → MYSQL5 |
| 故障恢复 | 业务、前态、收据、进度和下一跳标记同事务；同一源事务校验哈希；失败阻塞后续 NATS 交付 | PG 约束失败、杀进程重启、重复信封、位点丢失测试 |

独立目标数据库间的扇出未实现。一个 job 的多个目标表使用一个 sink 数据库；跨库需要独立 job，也就没有跨库事务原子性。聚合业务逻辑仍由示例 SQL trigger 定义，YAML 负责行映射与路由，不会自动推导任意聚合的逆变换。

## 1. 编译与离线验证

以下命令从仓库根目录运行。需要 Go 1.24.6 或更高版本、PowerShell 7.5。本工作区也可将 `go` 替换为 `./tmp/toolchain/go/bin/go.exe`，该可执行文件路径需按当前目录调整。

```powershell
Set-Location pgflo-pg_flo-33adfb2
go test ./...
go vet ./...
go build -buildvcs=false -o bin/pg_flo.exe .
./bin/pg_flo.exe job validate --job examples/uid-replay/forward.yaml
./bin/pg_flo.exe job validate --job examples/uid-replay/reverse.yaml
./bin/pg_flo.exe job preview --job examples/uid-replay/json-reverse.yaml --events examples/uid-replay/json-events.yaml
Set-Location ..
```

preview 的预期结果：目标 `legacy.orders_1`，原主键 `9007199254740993`，`customer_name=Bob`，对 `document.stock.reserved` 写入 7，`require-existing=true`。preview 只生成映射计划，运行时才检查 UID、epoch 和来源。

## 2. 启动本地实验环境

```powershell
docker compose version
docker compose -p pgflo-uid-lab -f cdc-replay-v2/compose.yaml -f pgflo-pg_flo-33adfb2/examples/uid-replay/compose.yaml up -d --wait
```

本地端口：MySQL 53306，PostgreSQL 18 为 56543，NATS JetStream 为 4242。NATS 使用命名卷。实验数据库使用 trust / 无密码并只绑定回环地址。保留元数据、slot、JetStream 和 MySQL adapter 的 offset/history 是恢复的前提。

## 3. 不依赖 Java 的真 WAL 测试

```powershell
Set-Location pgflo-pg_flo-33adfb2
$env:PGFLO_REPLAY_BINARY = (Resolve-Path bin/pg_flo.exe).Path
$env:PGFLO_REPLAY_TEST_DSN = 'host=127.0.0.1 port=56543 user=postgres dbname=postgres sslmode=disable'
go test -count=1 -v ./pkg/sinks ./tests/scenarios -run TestReplay -timeout 3m
```

测试独立创建临时数据库与 NATS 通道，结束后清理这些临时对象。包含真实 slot 导出快照、64 KB JSON 的 unchanged TOAST、部分 JSON 回写、kill/restart、离线期间 WAL、重复交付、丢失 NATS 位点、删除传输历史以及复制槽丢失的拒绝启动。

## 4. 手工 JSON 反向回写闭环

从仓库根目录执行，`psql` 必须在 PATH 中。使用全新 `json_source` / `json_target` 数据库。

```powershell
psql -X -h 127.0.0.1 -p 56543 -U postgres -d postgres -v ON_ERROR_STOP=1 -c 'CREATE DATABASE json_source'
psql -X -h 127.0.0.1 -p 56543 -U postgres -d postgres -v ON_ERROR_STOP=1 -c 'CREATE DATABASE json_target'
psql -X -h 127.0.0.1 -p 56543 -U postgres -d json_source -v ON_ERROR_STOP=1 -f pgflo-pg_flo-33adfb2/examples/uid-replay/json-source.sql
psql -X -h 127.0.0.1 -p 56543 -U postgres -d json_target -v ON_ERROR_STOP=1 -f pgflo-pg_flo-33adfb2/examples/uid-replay/json-target.sql

$exe='./pgflo-pg_flo-33adfb2/bin/pg_flo.exe'
$job='./pgflo-pg_flo-33adfb2/examples/uid-replay/json-reverse.yaml'
& $exe job setup --job $job
& $exe job ownership --job $job --node source --action register --uid 0 --writer PG --epoch 1
& $exe job ownership --job $job --node sink --action register --uid 0 --writer PG --epoch 1
& $exe job run --job $job
```

在另一终端提交业务事务，标记与业务 DML 必须在同一事务：

```powershell
psql -X -h 127.0.0.1 -p 56543 -U postgres -d json_source -v ON_ERROR_STOP=1 -c "BEGIN; SELECT cdc_meta.begin_write('json_demo',0,'PG',1,'json-business-1'); UPDATE app.orders SET reserved_qty=7 WHERE id=42; COMMIT;"
psql -X -h 127.0.0.1 -p 56543 -U postgres -d json_target -c 'SELECT id,uid,customer_name,document,barcode FROM legacy.orders_1'
```

预期 id 仍是 `9007199254740993`，姓名为 `Alice`（json_get + 自定义 trim_space），reserved 变成 7；available=30、unmanaged.code 与 barcode 保留。第一次更新不改大 JSON，pg_flo 依靠 setup 建立的前态恢复 TOAST。修改 customer_name 的样例事件可直接看 `json-events.yaml`；真实 SQL 可用 jsonb_set 修改源 payload 后提交另一个唯一 tx ID。

`job setup` 安装协议表/函数、创建 origin/publication/slot，并从 slot 导出的同一快照初始化源行前态。它不会复制或校验全部目标业务基线。以上 SQL 提供已对齐基线；接入已有库存表时，应隔离应用写入、完成一致基线对账，再 setup 和开放写入。已有 baseline 且 slot 丢失、已有 slot 却无完整 baseline，都会停止，不能用重复 setup 跳过历史。

## 5. MySQL ↔ PG 四跳库存验证

链路为 `MySQL → Java PG bridge → PG 原分表 → pg_flo forward → PG 合表 → pg_flo reverse → PG 原分表 → Java MySQL sink → MySQL`。保留的 channel / stage 名字中含 pgcat，是与既有 Java 适配器的协议兼容名称；PG 两跳不运行 pgcat。

需要前一阶段的两个适配器源码仓库，分支均为 `codex/uid-fenced-cdc`；先 install 公共 bridge，再 package MySQL sink。库存样例的四表数据、合并/拆分 trigger、publication 在 `cdc-replay-v2/sql/`。

```powershell
mvn -f ../cdc-pg-sink/tx-bridge/pom.xml install
mvn -f ../cdc-mysql-sink/tx-sink/pom.xml package
mvn -f ../cdc-pg-sink/tx-bridge/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath '-Dmdep.outputFile=target/flink-classpath.txt'

./cdc-replay-v2/scripts/Run-Lab.ps1 `
  -PgFlo ./pgflo-pg_flo-33adfb2/bin/pg_flo.exe `
  -PgSinkJar ../cdc-pg-sink/tx-bridge/target/cdc-tx-bridge-0.1.0-all.jar `
  -MysqlSinkJar ../cdc-mysql-sink/tx-sink/target/cdc-mysql-tx-sink-0.1.0-all.jar `
  -Psql psql -Mysql mysql -Java java `
  -FlinkClasspathFile ../cdc-pg-sink/tx-bridge/target/flink-classpath.txt
```

这是故障注入测试，要求全新的 `uid_mirror` / `uid_merged` / MySQL 实验库，不得指向已有业务数据库。脚本输出日志目录和 `result.txt`。测试末尾故意提交非法跨 UID 源事务，以验证目标完全回滚；后续重新测试需重新建立空实验环境和新的 RunDirectory。

实测入口使用的已构建适配器也在本工作区 `tmp/sink-uid-v2/cdc-pg-sink/tx-bridge/target/` 与 `tmp/sink-uid-v2/cdc-mysql-sink/tx-sink/target/`；这些临时构建产物不属于 Go 源码交付。

## 6. 切换命令与故障重试

`Switch-Uid.ps1` 是四跳库存环境的参考协调器，须让连接参数与 forward/reverse YAML 一致。它先冻结旧主，再同步其余节点，发送一次屏障，验收两跳收据，把经过验证的收据作为协调器证明写入各端，最后按“其他节点先、新主最后”激活。

```powershell
./pgflo-pg_flo-33adfb2/examples/uid-replay/Switch-Uid.ps1 `
  -PgFlo ./pgflo-pg_flo-33adfb2/bin/pg_flo.exe -Psql psql -Mysql mysql `
  -Writer MYSQL -Epoch 1 -Uid 1 -Barrier inventory-u1-e1
# 切回时 Writer=PG、Epoch=2，并使用新的屏障号。
```

同 UID 的协调操作应串行。中断后使用**相同** Writer/Epoch/Barrier 重试；不能换屏障绕过已冻结状态。各节点 CAS 和永久收据支持部分完成后恢复。参考协调器持有管理权限并负责跨节点证明，它不提供分布式共识、自动故障判主或旧主失联时的强制提升。

每笔应用写调用 `cdc_meta.begin_write(namespace,uid,writer,epoch,unique_tx_id)`；MySQL 案例调用 `begin_mysql_write`。业务账号必须通过受控存储过程或业务写服务访问，避免绕过门禁的直接 DML。PG 共享行锁保持到提交，freeze 的排他行锁等待在途事务完成。一个源事务只允许一个逻辑 UID，不能通过修改 UID 来迁移所有权；独立的 tenant_id / warehouse 路由键变更仍由映射计划处理。

## 7. 自定义转换函数

`cmd/transforms.go` 给出可运行的 `trim_space` 编译扩展，YAML 已实际使用。新增函数可在同类 init 中调用：

```go
mapping.RegisterExtension("business_v2", func(ctx mapping.TransformContext, value interface{}, args []string) (interface{}, error) {
    // 必须确定性执行；可读取 ctx.Row、SourceTable、RouteID，不得做外部 I/O。
    return value, nil
})
```

重新编译后在 transforms 引用函数名。单独嵌入 Engine 的程序也可调用 `engine.Register`。配置函数必须已注册；`job validate` 会拒绝未知函数。函数语义变化时更新 mapping.version，并按新通道重建基线；同一通道的源地址/表范围、配置、stream ID 或 origin 指纹变化会被拒绝。JSON patch 目前支持对象路径，数组读取可用 json_get，数组元素回写需编写业务函数或增加相应 patch 实现。

## 8. 本次边界

本版本按通道串行应用事务，多个 UID 可以拥有不同写主，但没有按 UID 并行调度。目标 PostgreSQL 内事务原子性已验证；全链路通过各跳的事务收据衔接，没有跨 MySQL/PG 的全局提交事务。反向样例只更新已有的四张库存分表，原始 ID 保留；新库存聚合的 INSERT/DELETE、共享组件分摊需另行定义业务规则。

Replay 模式不自动处理 DDL、TRUNCATE、源槽丢失或不完整基线；TRUNCATE 会停止捕获。NATS 事务不拆包，整笔事务须小于服务端 max_payload 并能放入内存，超限时发布失败且不推进源提交位置。JetStream 暂不提供在线安全清理协议，删除传输历史会被拒绝继续捕获。部署前仍需接入持久存储、权限隔离、延迟监控及实际表结构验收。

组件实现说明见 [DESIGN.zh-CN.md](DESIGN.zh-CN.md)，实测记录见 [VALIDATION.md](VALIDATION.md)。
