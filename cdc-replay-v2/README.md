# UID 写权与四段 CDC 链路

同一个 `(scope_namespace, uid)` 在任一时刻只由 MySQL 或 PG 合表的一端接受应用写入。另一端保留用来切换的副本。不同 UID 可以处于不同代次、由不同端写入。

| 链路 | 处理入口 | 接受并应用的事务 | 只确认进度的回声 |
| --- | --- | --- | --- |
| MySQL → PG 分表 | `cdc-pg-sink/tx-bridge` | `MYSQL / native` | `PG / mysql-sink` |
| PG 分表 → PG 合表 | `pgcat-yaml` forward | `MYSQL / pg-sink` | 反向 origin + `PG / pgcat-reverse` |
| PG 合表 → PG 分表 | `pgcat-yaml` reverse | `PG / native` | 正向 origin + `MYSQL / pgcat-forward` |
| PG 分表 → MySQL | `cdc-mysql-sink/tx-sink` | `PG / pgcat-reverse` | `MYSQL / pg-sink` |

`cdc_meta.tx_context` 和业务事务一起提交。后续每一跳使用新的 `marker_id`，保留原始 `root_tx_id`、UID、代次、主写端和屏障号。PG 间还要求 replication origin 与阶段匹配。跨 MySQL 时，事务标记提供跨引擎来源证据。回声可以提交同步进度，禁止重发来源标记。来自未登记阶段或无标记的业务事件会停在故障位置。

## 写权与独立 schema

| 对象 | 用途 |
| --- | --- |
| `cdc_meta.write_ownership` | 每端保存 `(namespace, uid, writer, epoch, state, barrier)`。应用持有共享行锁直到业务事务提交。切换更新等待在途应用事务释放锁。 |
| `cdc_meta.tx_context` | 每个已捕获业务事务一条只追加的标记。只设置屏障的事务只写这一条标记。 |
| `cdc_meta.applied_tx` | JDBC 应用收据。以通道、源流身份及源事务 ID/位置派生的键去重，并验证 payload 哈希。 |
| `cdc_meta.apply_progress` | 绑定逻辑通道与源流，记录启动和最近成功提交的位置。数据库收据与 Debezium offset 文件配合处理“目标已提交、源位置尚未保存”的重启。 |
| `cdc_meta.cutover_step` | 记录屏障在哪一条链路完成，包含该链路的源位置。激活下一任主写前需验收完整路径。 |
| `_pgcat_yaml` | 既有映射服务保存行状态、可选身份旁路、checkpoint 和配置指纹。PG 两跳继续复用。 |

MySQL 中 `cdc_meta` 是独立 database，与历史业务表必须处于同一 MySQL 实例且均使用 InnoDB。PG 中它是 schema，各数据库内分别建立。业务表无需增加 CDC 追踪列。PG 分表和合表捕获表要配置 `REPLICA IDENTITY FULL`。示例把 PG 映射所需来源列放在投影和反向接收表中。

“合四张表”描述投影依赖；跨表原子性以源事务实际改动为边界。一次 MySQL 事务更新四张表，目标用一个事务应用全部四张；只更新库存数量时，合表从其余已提交分量补齐。PG 主写产生一个合表变更，反向目标在同一事务更新四张已有分表并保留历史主键。双方事务在当前版本都必须属于同一 UID。共享组件映射、一个 UID 多个聚合行、历史 ID 分配、级联删除、未知列和复杂类型应分别制定业务规则。

本案例直接读取带事务标记的业务 WAL/binlog。需要可逆、稳定的行模型，另外可以按业务需要增加 outbox 来承载派生事件、合并后的稳定输出载荷或与业务更新一起生成的删除意图。pgcat 提供 WAL 应用、路由、回声判断和事务进度；业务映射仍需提供拆分目标、合并键及历史记录的身份规则。

## 切换顺序

以 UID 1 从 `MYSQL/1` 切到 `PG/2` 为例。应用进入业务写事务后调用 `cdc_meta.begin_mysql_write` 或 `cdc_meta.begin_pg_write`，传入服务端生成的全局唯一业务事务 ID。函数或存储过程先校验写权和代次，再插入标记。业务 DML、标记和锁须共用同一连接与事务。失败时回滚整个事务。在生产通过业务写入服务或只授权受保护的存储过程强制实行，业务账号应无法绕过门禁直接向已纳管表写入。

1. 串行化该 UID 的协调操作。在旧主上用 CAS 将 `(MYSQL,1,ACTIVE)` 更新为 `(MYSQL,1,DRAINING,B)`，等待此前已持锁的事务完成。向两个其余节点写入相同排空状态。
2. 只在旧主写入一次屏障标记 `B`。已有旧代事务仍可按日志顺序应用，直到相应通道在同一目标事务中写入 `B` 收据。屏障后新到的旧代业务事务被拒绝。
3. 确认 `mysql_to_mirror` 的收据在分表端，`pgcat_forward` 的收据在合表端，再核对该 UID 的业务结果。先在非新主节点安装 `(PG,2,ACTIVE)`，最后激活合表并更新应用路由。反向切回需要 `pgcat_reverse` 和 `mirror_to_mysql` 屏障收据，并将代次增到 3。
4. 中途故障保持旧主写入关闭。通过 CAS、原有屏障记录和实际进度恢复协调。已写入的屏障重复查询，禁止换一个屏障继续旧代切换。源故障且最后事务无法确认送达时，先隔离旧主并对账，根据可恢复位置完成恢复决策。

## 构建和复现

需要 Go（版本见 `pgcat-yaml/go.mod`）、JDK 17 或 21、Maven、PowerShell 7.5、`psql` 与 `mysql` 命令。三个工作区应检出以下分支。

| 仓库 | 分支 | 构建 |
| --- | --- | --- |
| `cdc-pg-sink` | `codex/uid-fenced-cdc` | `mvn -f tx-bridge/pom.xml install`，产生 `tx-bridge/target/cdc-tx-bridge-0.1.0-all.jar` 并安装公共模块 |
| `cdc-mysql-sink` | `codex/uid-fenced-cdc` | `mvn -f tx-sink/pom.xml package`，产生 `tx-sink/target/cdc-mysql-tx-sink-0.1.0-all.jar` |
| pgcat | `codex/uid-replay-protocol-v2` | 在 `pgcat-yaml` 目录执行 `go test ./...` 和 `go build -o ../pgcat-yaml-bin .`（Windows 输出名用 `pgcat-yaml-bin.exe`） |

启动一次性本地实验库，MySQL 端口 53306，PG 端口 56543。也可运行同端口的全新本机实例，开启 MySQL ROW/FULL binlog 和 PG 逻辑复制。

```powershell
docker compose -p uid-cdc-lab -f cdc-replay-v2/compose.yaml up -d --wait
./cdc-replay-v2/scripts/Run-Lab.ps1 -Pgcat ./pgcat-yaml-bin.exe -PgSinkJar ../cdc-pg-sink/tx-bridge/target/cdc-tx-bridge-0.1.0-all.jar -MysqlSinkJar ../cdc-mysql-sink/tx-sink/target/cdc-mysql-tx-sink-0.1.0-all.jar -Java (Join-Path $env:JAVA_HOME 'bin/java.exe')
docker compose -p uid-cdc-lab -f cdc-replay-v2/compose.yaml down -v
```

Linux 使用 `pwsh`，二进制路径去掉 `.exe`。脚本初始化样例数据库，启动四条链路，运行断言，退出时停止脚本启动的进程。局部失败时保留每段日志、offset 和 history 文件。脚本末尾故意在实验源提交非法混合 UID 事务。重复实验使用一次性空库和全新工作目录。

| 验收项 | 检查 |
| --- | --- |
| MySQL 四表 → PG 合表 | `101,102,103,104`；仅正向写入标记、回声有收据。 |
| MySQL → PG 主切换 | 屏障两跳先收敛，MySQL 旧代写被拒，PG 合表更新反向写至两套分表。 |
| 跨表身份和 UID 隔离 | 历史四个主键保持 `1,2,3,4`；UID 2 可独立 DELETE/INSERT 且由 MySQL 主写。 |
| PG → MySQL 故障切回 | PG 日志消费进程先重启，反向屏障仍可送达。新代为 3。MySQL 只改 stock 后，各节点其余三项保持 PG 旧主最后值。 |
| 目标第二表写入失败 | 第一表回滚，没有应用收据。修复临时 CHECK 后重启，从持久 offset 重读并提交一次。 |
| 恢复已过期的源 offset | 同一事务再次交付时原标记仍只有一条，作业可继续应用后续事务。 |
| 错误的混合 UID | 作业停止在该事务，目标两个 UID 没有部分更新。 |
| offset 文件丢失或截断 | 作业拒绝启动，避免重新从当前源位置读起。 |

## 接入现有 Flink 部署

`mysql-to-pg-all-prod-k8sHa.yaml` 类 Pipeline YAML 保留原有行为。用它部署保留的表，并从该任务的捕获、路由和重放范围剔除纳管表，核实实际写目标确实互斥。纳管表改用下述两个专用 JAR job，分别固定并行度 1。示例用已挂载的 JAR、专用 YAML 和 Secret 环境变量，路径在实际执行的 TaskManager 中必须存在。

```bash
flink run -d -c com.example.flinkcdc.tx.CdcTxJob /opt/cdc/cdc-tx-bridge-0.1.0-all.jar --config /etc/cdc/mysql-to-mirror.yaml
flink run -d -c com.example.flinkcdc.tx.mysql.MysqlTxJob /opt/cdc/cdc-mysql-tx-sink-0.1.0-all.jar --config /etc/cdc/mirror-to-mysql.yaml
```

两个 JAR 也可以用 `java -jar ... --config ...` 独立部署，实验采用这一入口。Flink 宿主是 1.20.1；事务顺序、重试和恢复以 Debezium 日志位置及 JDBC 收据为准。每条链路挂独立持久卷，YAML 的 offset/history 路径用绝对路径。单活、PVC 持久化、日志保留和生产 Flink/Kubernetes 重启测试要纳入上线验收。保留 JDBC 找到行语义，MySQL URL 避免 `useAffectedRows=true`。真实业务列启用前需验证类型、before image 与主键约束，示例只使用整数列。

## 初始化、恢复和上线条件

1. 明确纳管表、发布范围、稳定键、单 UID 业务事务与反向允许字段。暂停同一对象的旧同步写路径。在写入隔离期间建立一致基线、登记三端写权、配置 publication/slot 并启动链路。MySQL 使用 `schema_only` 启动，PG pgcat 使用 `seed`。用受控业务探针确认首个事务、offset/history 均持久化，再开放常规写入。
2. 生产应用、JDBC Sink、pgcat 和切换控制各用独立账号授权。复制和 SQL apply 账号才允许写中间层或阶段标记。不要把实验的 root、trust 和无密码配置带到生产。PG 业务合表和四张分表必须通过这四条通道走完，普通连接直接写分表副本会被视为入站异常。
3. 事故后保留 source stream ID、position、PG slot、origin、应用收据以及 offset/history。纠正目标约束或映射后从原位置恢复。同一事务重复交付由收据检查。启动状态与 offset 不一致时停止，通过备份及日志证明位置，或冻结重做基线后再配新通道。禁止随意删除 offset、删槽或重建通道跳过未处理的事务。
4. 检查通道延迟、源日志滞留、barrier 等待时间、故障事务、历史表键/约束和元数据增长。标记表使用仅追加语义；在线清理、双机故障后选主协调、自定义反向 INSERT/DELETE 和生产表映射属于投产接入项。示例反向路径只更新已存在的四张分表记录，缺少原始身份或没有确认删除策略时停止。
