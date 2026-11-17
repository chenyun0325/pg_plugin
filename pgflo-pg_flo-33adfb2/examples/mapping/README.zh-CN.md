# pg_flo YAML 映射完整闭环示例

本目录可同时验证表/字段映射、水平分片、垂直拆分、N:1 合表、JSON key 抽取、主键来源隔离、删除归属和 PostgreSQL 双向防回环。

> 这个 pg_flo 版本的采集端仍是 PostgreSQL。用户给出的 `mysql-shards-to-postgres18-wms-fix.yaml` 是 Flink CDC 配置，本文借用了其中的 `source / sink / route / transform / pipeline` 思路，但不能把 pg_flo 直接声明成 MySQL binlog 采集器。MySQL→PostgreSQL 仍应使用 Flink CDC/Debezium，或另写 MySQL source adapter。

## 1. 环境

- Go 1.24.6（`go.mod` 声明 Go 1.24/toolchain 1.24.6）
- Docker 与 Docker Compose v2
- 可用端口：4222、8222、55432、55433

PowerShell：

```powershell
cd C:\path\to\pgflo-pg_flo-33adfb2
docker compose -f examples/mapping/docker-compose.yml up -d
docker compose -f examples/mapping/docker-compose.yml ps

go version
go test ./...
New-Item -ItemType Directory -Force bin | Out-Null
go build -trimpath -o bin/pg_flo.exe .
```

Linux/macOS：

```bash
cd /path/to/pgflo-pg_flo-33adfb2
docker compose -f examples/mapping/docker-compose.yml up -d
go test ./...
mkdir -p bin
go build -trimpath -o bin/pg_flo .
```

## 2. 不连数据库先验证 YAML 和转换

PowerShell：

```powershell
.\bin\pg_flo.exe job validate --job examples/mapping/pgflo-job.yaml
.\bin\pg_flo.exe job preview `
  --job examples/mapping/pgflo-job.yaml `
  --events examples/mapping/events.yaml
```

预期：

- INSERT tenant=2 生成 `legacy.orders_0` 和 `app.order_detail` 两条目标变更；
- UPDATE tenant 2→3 生成 `orders_0 DELETE`、`orders_1 INSERT`、`order_detail UPDATE`；
- DELETE 生成当前水平分片与详情表的两个 DELETE；
- 两个源分表的 `id=1` 在 `app.all_legacy_orders` 中拥有不同 `_pgflo_source_table`，不会互相覆盖；
- `payload.customer.name`、`payload.note`、`payload.outbound_id` 分别被抽取，`outbound_id` 转成 int64。

## 3. 启动真实正向链路

在一个独立终端运行：

```powershell
.\bin\pg_flo.exe job run --job examples/mapping/pgflo-job.yaml
```

首次运行是 `copy-and-stream`。等待日志显示 copy 完成，然后检查目标：

```powershell
docker compose -f examples/mapping/docker-compose.yml exec target-db `
  psql -U postgres -d target_db -c "TABLE legacy.orders_0"

docker compose -f examples/mapping/docker-compose.yml exec target-db `
  psql -U postgres -d target_db -c "TABLE app.order_detail"

docker compose -f examples/mapping/docker-compose.yml exec target-db `
  psql -U postgres -d target_db -c "SELECT _pgflo_source_table,order_id,customer_name,amount FROM app.all_legacy_orders ORDER BY _pgflo_source_table"
```

最后一个查询应返回两行 `order_id=1`，来源分别是 `public.legacy_orders_0` 和 `public.legacy_orders_1`。

## 4. 验证水平迁移、垂直拆分、JSON 转换和 DELETE

写入并迁移分片：

```powershell
docker compose -f examples/mapping/docker-compose.yml exec source-db `
  psql -U postgres -d source_db -c "INSERT INTO public.orders(id,tenant_id,payload) VALUES (202,2,'{\"customer\":{\"name\":\"Carol\"},\"note\":\"new\",\"outbound_id\":8001}')"

docker compose -f examples/mapping/docker-compose.yml exec source-db `
  psql -U postgres -d source_db -c "UPDATE public.orders SET tenant_id=3,payload='{\"customer\":{\"name\":\"Dora\"},\"note\":\"moved\",\"outbound_id\":8002}' WHERE id=202"
```

查询结果：

```powershell
docker compose -f examples/mapping/docker-compose.yml exec target-db `
  psql -U postgres -d target_db -c "SELECT order_id FROM legacy.orders_0 WHERE order_id=202; SELECT order_id,customer_name FROM legacy.orders_1 WHERE order_id=202; SELECT order_id,note,outbound_id FROM app.order_detail WHERE order_id=202"
```

预期 `orders_0=0 行`、`orders_1=(202,Dora)`、详情为 `(202,moved,8002)`。再删除：

```powershell
docker compose -f examples/mapping/docker-compose.yml exec source-db `
  psql -U postgres -d source_db -c "DELETE FROM public.orders WHERE id=202"

docker compose -f examples/mapping/docker-compose.yml exec target-db `
  psql -U postgres -d target_db -c "SELECT (SELECT count(*) FROM legacy.orders_1 WHERE order_id=202) AS shard_rows,(SELECT count(*) FROM app.order_detail WHERE order_id=202) AS detail_rows"
```

两个计数都应为 0。

## 5. 验证双向回环防护

保持正向 job 运行，在第二个终端启动反向 job：

```powershell
.\bin\pg_flo.exe job run --job examples/mapping/pgflo-job-reverse.yaml
```

正向初始数据不会被反向捕获，因为目标 Sink 的事务被标记为 `pgflo_from_source_demo`，反向 pgoutput 使用 `origin=none`。现在在目标做一笔真正的本地业务修改：

```powershell
docker compose -f examples/mapping/docker-compose.yml exec target-db `
  psql -U postgres -d target_db -c "UPDATE legacy.orders_0 SET customer_name='Local-Edit',payload=jsonb_set(payload,'{customer,name}','\"Local-Edit\"') WHERE order_id=101"

docker compose -f examples/mapping/docker-compose.yml exec source-db `
  psql -U postgres -d source_db -c "SELECT id,tenant_id,payload FROM public.orders WHERE id=101"
```

源端应出现 `Local-Edit`。反向 Sink 使用 `pgflo_from_target_demo`，所以正向捕获端也会过滤这笔回放，事件不会 A→B→A 无限循环。检查两个逻辑槽持续前进且目标行数不增长：

```powershell
docker compose -f examples/mapping/docker-compose.yml exec source-db `
  psql -U postgres -d source_db -c "SELECT slot_name,active,confirmed_flush_lsn FROM pg_replication_slots"

docker compose -f examples/mapping/docker-compose.yml exec target-db `
  psql -U postgres -d target_db -c "SELECT slot_name,active,confirmed_flush_lsn FROM pg_replication_slots"
```

## 6. 密码与部署

示例默认密码只适合本机测试。YAML 支持 `${NAME}`（必须设置）与 `${NAME:-default}`。生产示例：

```powershell
$env:PGFLO_SOURCE_PASSWORD = Read-Host -MaskInput
$env:PGFLO_TARGET_PASSWORD = Read-Host -MaskInput
```

生产建议把 replicator 和 worker 分进程运行：

```powershell
.\bin\pg_flo.exe job replicator --job examples/mapping/pgflo-job.yaml
.\bin\pg_flo.exe job worker --job examples/mapping/pgflo-job.yaml
```

JetStream `max-age: 0s` 避免按时间自动丢未消费事件，但仍必须监控磁盘、consumer lag、复制槽 WAL 积压和 NATS 最大消息。每个 group 只运行一个有序 worker。

## 7. 清理

以下命令会删除本示例的 PostgreSQL/NATS Docker volumes，数据不可恢复：

```powershell
docker compose -f examples/mapping/docker-compose.yml down -v
```
