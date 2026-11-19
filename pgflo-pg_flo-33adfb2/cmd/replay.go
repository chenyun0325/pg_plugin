package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/pgflo/pg_flo/pkg/jobconfig"
	"github.com/pgflo/pg_flo/pkg/pgflonats"
	"github.com/pgflo/pg_flo/pkg/replay"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/sinks"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/spf13/cobra"
)

func jobConnection(job *jobconfig.Job, node string) (*pgx.Conn, error) {
	host, port, database, user, password := job.Sink.Host, job.Sink.Port, job.Sink.Database, job.Sink.User, job.Sink.Password
	if node == "source" {
		host, port, database, user, password = job.Source.Host, job.Source.Port, job.Source.Database, job.Source.User, job.Source.Password
	}
	// Parse the complete address so pgx's TLS fallbacks use this node too.
	return pgx.Connect(context.Background(), utils.PostgresURL(host, port, database, user, password))
}

func newReplaySetupCommand() *cobra.Command {
	c := &cobra.Command{Use: "setup", Short: "Install replay metadata and seed row state from a slot-exported snapshot (business baseline must already agree)", RunE: func(c *cobra.Command, _ []string) error {
		job, err := loadJobFlag(c)
		if err != nil {
			return err
		}
		if job.Replay == nil {
			return fmt.Errorf("setup requires replay-v2")
		}
		ctx := c.Context()
		source, err := jobConnection(job, "source")
		if err != nil {
			return err
		}
		defer source.Close(ctx)
		target, err := jobConnection(job, "sink")
		if err != nil {
			return err
		}
		defer target.Close(ctx)
		for _, conn := range []*pgx.Conn{source, target} {
			if _, err = conn.Exec(ctx, replay.Schema); err != nil {
				return err
			}
		}
		var originExists bool
		err = target.QueryRow(ctx, `SELECT pg_replication_origin_oid($1) IS NOT NULL`, job.Sink.Origin).Scan(&originExists)
		if err != nil {
			return err
		}
		if !originExists {
			if _, err = target.Exec(ctx, `SELECT pg_replication_origin_create($1)`, job.Sink.Origin); err != nil {
				return err
			}
		}
		binding, err := sinks.NewReplaySink(sinks.PostgresSinkConfig{TargetHost: job.Sink.Host, TargetPort: job.Sink.Port, TargetDBName: job.Sink.Database, TargetUser: job.Sink.User, TargetPassword: job.Sink.Password, ReplicationOrigin: job.Sink.Origin}, *job.Replay, job.Source.Group, job.Mapping)
		if err != nil {
			return err
		}
		defer binding.Close()
		slot := replicator.GeneratePublicationName(job.Source.Group)
		var seeded, exists bool
		if err = target.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pgflo_meta.replay_baseline WHERE channel=$1 AND slot=$2)`, job.Source.Group, slot).Scan(&seeded); err != nil {
			return err
		}
		if err = source.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name=$1 AND database=current_database())`, slot).Scan(&exists); err != nil {
			return err
		}
		if seeded {
			if !exists {
				return fmt.Errorf("baseline exists but source slot is lost; restore or rebaseline under a new channel")
			}
			fmt.Println("Replay baseline already installed")
			return nil
		}
		if exists {
			return fmt.Errorf("slot exists without committed baseline; reconcile before retrying setup")
		}
		var quoted []string
		for _, table := range job.Source.Tables {
			parts := strings.Split(table, ".")
			if len(parts) == 1 {
				parts = []string{job.Source.Schema, table}
			}
			quoted = append(quoted, pgx.Identifier(parts).Sanitize())
		}
		var publication bool
		if err = source.QueryRow(ctx, `SELECT EXISTS(SELECT FROM pg_publication WHERE pubname=$1)`, slot).Scan(&publication); err != nil {
			return err
		}
		if !publication {
			if _, err = source.Exec(ctx, "CREATE PUBLICATION "+pgx.Identifier{slot}.Sanitize()+" FOR TABLE "+strings.Join(quoted, ",")); err != nil {
				return err
			}
		}
		rc := replicator.NewReplicationConnection(job.ReplicatorConfig())
		if err = rc.Connect(ctx); err != nil {
			return err
		}
		defer rc.Close(ctx)
		point, err := rc.CreateReplicationSlot(ctx, slot)
		if err != nil {
			return err
		}
		read, err := source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
		if err != nil {
			return err
		}
		defer read.Rollback(ctx)
		if _, err = read.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(point.SnapshotName, "'", "''")+"'"); err != nil {
			return err
		}
		write, err := target.Begin(ctx)
		if err != nil {
			return err
		}
		defer write.Rollback(ctx)
		for _, table := range job.Source.Tables {
			if table == replay.ContextTable {
				continue
			}
			parts := strings.Split(table, ".")
			if len(parts) == 1 {
				parts = []string{job.Source.Schema, table}
			}
			name := parts[0] + "." + parts[1]
			var keys []string
			err = read.QueryRow(ctx, `SELECT ARRAY(SELECT a.attname FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,n) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE i.indrelid=$1::regclass AND i.indisprimary ORDER BY k.n)`, pgx.Identifier(parts).Sanitize()).Scan(&keys)
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				return fmt.Errorf("source needs primary key: %s", name)
			}
			var columns []string
			for _, key := range keys {
				columns = append(columns, pgx.Identifier{key}.Sanitize())
			}
			rows, err := read.Query(ctx, "SELECT json_build_array("+strings.Join(columns, ",")+")::text,row_to_json(t)::text FROM "+pgx.Identifier(parts).Sanitize()+" t")
			if err != nil {
				return err
			}
			for rows.Next() {
				var pk, data string
				if err = rows.Scan(&pk, &data); err != nil {
					rows.Close()
					return err
				}
				// Canonicalize composite keys without float64 round-tripping.
				var keyParts []json.RawMessage
				if err = json.Unmarshal([]byte(pk), &keyParts); err != nil {
					rows.Close()
					return err
				}
				encoded, _ := json.Marshal(keyParts)
				if _, err = write.Exec(ctx, `INSERT INTO pgflo_meta.source_rows VALUES($1,$2,$3,$4::jsonb)`, job.Source.Group, name, string(encoded), data); err != nil {
					rows.Close()
					return err
				}
			}
			if err = rows.Err(); err != nil {
				rows.Close()
				return err
			}
			rows.Close()
		}
		if _, err = write.Exec(ctx, `INSERT INTO pgflo_meta.replay_baseline VALUES($1,$2,$3::pg_lsn)`, job.Source.Group, slot, point.ConsistentPoint); err != nil {
			return err
		}
		if err = write.Commit(ctx); err != nil {
			return err
		}
		fmt.Println("Replay baseline seeded at", point.ConsistentPoint, "slot", slot)
		return nil
	}}
	c.Flags().String("job", "", "Job YAML")
	_ = c.MarkFlagRequired("job")
	return c
}

func requireReplayBaseline(job *jobconfig.Job) error {
	if job.Replay == nil {
		return nil
	}
	conn, err := jobConnection(job, "sink")
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var ready bool
	err = conn.QueryRow(context.Background(), `SELECT EXISTS(SELECT FROM pgflo_meta.replay_baseline WHERE channel=$1 AND slot=$2)`, job.Source.Group, replicator.GeneratePublicationName(job.Source.Group)).Scan(&ready)
	if err != nil || !ready {
		return fmt.Errorf("replay baseline missing; execute job setup after aligning the initial business data: %v", err)
	}
	return nil
}

func validateReplayTransport(job *jobconfig.Job, client *pgflonats.NATSClient) error {
	if job.Replay == nil {
		return nil
	}
	ctx := context.Background()
	target, err := jobConnection(job, "sink")
	if err != nil {
		return err
	}
	defer target.Close(ctx)
	var baseline string
	err = target.QueryRow(ctx, `SELECT start_lsn::text FROM pgflo_meta.replay_baseline WHERE channel=$1`, job.Source.Group).Scan(&baseline)
	if err != nil {
		return err
	}
	start, err := pglogrepl.ParseLSN(baseline)
	if err != nil {
		return err
	}
	source, err := jobConnection(job, "source")
	if err != nil {
		return err
	}
	defer source.Close(ctx)
	var publicationOK bool
	err = source.QueryRow(ctx, `SELECT NOT puballtables AND pubinsert AND pubupdate AND pubdelete AND NOT pubviaroot FROM pg_publication WHERE pubname=$1`, replicator.GeneratePublicationName(job.Source.Group)).Scan(&publicationOK)
	if err != nil || !publicationOK {
		return fmt.Errorf("replay publication missing or not publishing all row operations: %v", err)
	}
	wanted := map[string]bool{}
	for _, table := range job.Source.Tables {
		if !strings.Contains(table, ".") {
			table = job.Source.Schema + "." + table
		}
		wanted[table] = true
	}
	rows, err := source.Query(ctx, `SELECT p.schemaname||'.'||p.tablename,
	 p.rowfilter IS NULL AND p.attnames=ARRAY(SELECT a.attname FROM pg_attribute a WHERE a.attrelid=(quote_ident(p.schemaname)||'.'||quote_ident(p.tablename))::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum)::name[]
	 FROM pg_publication_tables p WHERE pubname=$1`, replicator.GeneratePublicationName(job.Source.Group))
	if err != nil {
		return err
	}
	for rows.Next() {
		var name string
		var full bool
		if err = rows.Scan(&name, &full); err != nil {
			rows.Close()
			return err
		}
		if !wanted[name] || !full {
			rows.Close()
			return fmt.Errorf("replay publication table/filter/column scope differs: %s", name)
		}
		delete(wanted, name)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(wanted) != 0 {
		return fmt.Errorf("replay publication is missing configured tables")
	}
	var confirmed string
	err = source.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1 AND database=current_database() AND plugin='pgoutput' AND restart_lsn IS NOT NULL`, replicator.GeneratePublicationName(job.Source.Group)).Scan(&confirmed)
	if err != nil {
		return fmt.Errorf("replay source slot missing/invalid; restore or rebaseline: %w", err)
	}
	flush, err := pglogrepl.ParseLSN(confirmed)
	if err != nil {
		return err
	}
	state, err := client.GetState()
	if err != nil {
		return err
	}
	if state.LSN == 0 && flush > start {
		return fmt.Errorf("replay capture offset missing after source slot advanced; restore NATS state instead of skipping history")
	}
	if state.LSN != 0 && state.LSN < start {
		return fmt.Errorf("replay capture offset predates its baseline")
	}
	stream, err := client.JetStream().StreamInfo("pgflo_" + job.Source.Group + "_stream")
	if err != nil {
		return err
	}
	// Limits retention keeps acknowledged messages too. Any hole indicates an
	// operator deletion or changed retention policy, and invalidates continuity.
	if stream.State.NumDeleted > 0 || (stream.State.Msgs > 0 && stream.State.FirstSeq != 1) || (state.LSN != 0 && stream.State.Msgs == 0) {
		return fmt.Errorf("replay transport history missing; restore JetStream or rebaseline under a new channel")
	}
	return nil
}

func newOwnershipCommand() *cobra.Command {
	c := &cobra.Command{Use: "ownership", Short: "Inspect or CAS a UID cutover on one node; activation requires durable barrier receipts", RunE: func(c *cobra.Command, _ []string) error {
		job, err := loadJobFlag(c)
		if err != nil {
			return err
		}
		if job.Replay == nil {
			return fmt.Errorf("ownership requires replay-v2")
		}
		node, _ := c.Flags().GetString("node")
		if node != "source" && node != "sink" {
			return fmt.Errorf("node must be source or sink")
		}
		conn, err := jobConnection(job, node)
		if err != nil {
			return err
		}
		defer conn.Close(c.Context())
		action, _ := c.Flags().GetString("action")
		uid, _ := c.Flags().GetInt64("uid")
		epoch, _ := c.Flags().GetInt64("epoch")
		writer, _ := c.Flags().GetString("writer")
		barrier, _ := c.Flags().GetString("barrier")
		next, _ := c.Flags().GetString("next-writer")
		channels, _ := c.Flags().GetStringSlice("channels")
		scope := job.Replay.Namespace
		switch action {
		case "status":
			var raw string
			err = conn.QueryRow(c.Context(), `SELECT row_to_json(t)::text FROM cdc_meta.write_ownership t WHERE scope_namespace=$1 AND uid=$2`, scope, uid).Scan(&raw)
			if err == nil {
				fmt.Println(raw)
			}
		case "register":
			_, err = conn.Exec(c.Context(), `INSERT INTO cdc_meta.write_ownership VALUES($1,$2,$3,$4,'ACTIVE',NULL)`, scope, uid, writer, epoch)
		case "freeze":
			_, err = conn.Exec(c.Context(), `SELECT cdc_meta.freeze_writer($1,$2,$3,$4,$5)`, scope, uid, writer, epoch, barrier)
		case "barrier":
			stage, _ := c.Flags().GetString("stage")
			_, err = conn.Exec(c.Context(), `SELECT cdc_meta.emit_barrier($1,$2,$3,$4,$5,$6)`, scope, uid, writer, epoch, barrier, stage)
		case "activate":
			_, err = conn.Exec(c.Context(), `SELECT cdc_meta.activate_writer($1,$2,$3,$4,$5,$6,$7,$8)`, scope, uid, writer, epoch, next, epoch+1, barrier, channels)
		default:
			return fmt.Errorf("action must be status, register, freeze, barrier, activate")
		}
		return err
	}}
	c.Flags().String("job", "", "Job YAML")
	_ = c.MarkFlagRequired("job")
	c.Flags().String("node", "sink", "source or sink")
	c.Flags().String("action", "status", "status/register/freeze/barrier/activate")
	c.Flags().Int64("uid", -1, "UID (including zero)")
	_ = c.MarkFlagRequired("uid")
	c.Flags().Int64("epoch", 0, "Expected current epoch")
	c.Flags().String("writer", "", "Expected writer MYSQL/PG")
	c.Flags().String("barrier", "", "Durable barrier ID")
	c.Flags().String("next-writer", "", "New writer MYSQL/PG")
	c.Flags().StringSlice("channels", nil, "Required local barrier channels")
	c.Flags().String("stage", "native", "Source barrier stage")
	return c
}
