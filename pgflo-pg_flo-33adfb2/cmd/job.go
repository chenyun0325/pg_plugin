package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/pgflo/pg_flo/pkg/jobconfig"
	"github.com/pgflo/pg_flo/pkg/mapping"
	"github.com/pgflo/pg_flo/pkg/pgflonats"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/sinks"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/pgflo/pg_flo/pkg/worker"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

func newJobCommand() *cobra.Command {
	jobCmd := &cobra.Command{Use: "job", Short: "Run a complete YAML-defined replication job"}
	validateCmd := &cobra.Command{
		Use: "validate", Short: "Validate a job without connecting to external services",
		RunE: func(cmd *cobra.Command, _ []string) error {
			job, err := loadJobFlag(cmd)
			if err != nil {
				return err
			}
			fmt.Println("VALID:", job.RedactedSummary())
			return nil
		},
	}
	previewCmd := &cobra.Command{
		Use: "preview", Short: "Apply mapping YAML to sample CDC events and print target mutations",
		RunE: runJobPreview,
	}
	replicatorCmd := &cobra.Command{
		Use: "replicator", Short: "Run only the source capture component",
		RunE: runJobReplicator,
	}
	workerCmd := &cobra.Command{
		Use: "worker", Short: "Run only the mapping and sink component",
		RunE: runJobWorker,
	}
	runCmd := &cobra.Command{
		Use: "run", Short: "Run source capture and worker in one process",
		RunE: runCompleteJob,
	}
	for _, command := range []*cobra.Command{validateCmd, previewCmd, replicatorCmd, workerCmd, runCmd} {
		command.Flags().String("job", "", "Path to pgflo/v1 ReplicationJob YAML")
		if err := command.MarkFlagRequired("job"); err != nil {
			panic(err)
		}
	}
	previewCmd.Flags().String("events", "", "Path to sample CDC events YAML")
	if err := previewCmd.MarkFlagRequired("events"); err != nil {
		panic(err)
	}
	jobCmd.AddCommand(validateCmd, previewCmd, replicatorCmd, workerCmd, runCmd, newReplaySetupCommand(), newOwnershipCommand())
	return jobCmd
}

func loadJobFlag(cmd *cobra.Command) (*jobconfig.Job, error) {
	path, err := cmd.Flags().GetString("job")
	if err != nil {
		return nil, err
	}
	return jobconfig.Load(path)
}

func createJobNATS(job *jobconfig.Job) (*pgflonats.NATSClient, error) {
	return pgflonats.NewNATSClientWithOptions(
		job.NATS.URL,
		fmt.Sprintf("pgflo_%s_stream", job.Source.Group),
		job.Source.Group,
		pgflonats.StreamOptions{MaxAge: job.NATSMaxAge(), Replicas: job.NATS.Replicas},
	)
}

func createJobReplicator(job *jobconfig.Job, natsClient *pgflonats.NATSClient) (replicator.Replicator, error) {
	if err := requireReplayBaseline(job); err != nil {
		return nil, err
	}
	if err := validateReplayTransport(job, natsClient); err != nil {
		return nil, err
	}
	var factory replicator.Factory
	switch job.Source.Mode {
	case "copy":
		factory = &replicator.CopyAndStreamReplicatorFactory{MaxCopyWorkersPerTable: job.Source.MaxCopyWorkers, CopyOnly: true}
	case "copy-and-stream":
		factory = &replicator.CopyAndStreamReplicatorFactory{MaxCopyWorkersPerTable: job.Source.MaxCopyWorkers}
	default:
		factory = &replicator.StreamReplicatorFactory{}
	}
	return factory.CreateReplicator(job.ReplicatorConfig(), natsClient)
}

func createJobWorker(job *jobconfig.Job, natsClient *pgflonats.NATSClient) (*worker.Worker, sinks.ManagedSink, error) {
	if err := requireReplayBaseline(job); err != nil {
		return nil, nil, err
	}
	mapper, err := mapping.NewEngine(job.Mapping)
	if err != nil {
		return nil, nil, err
	}
	connection := sinks.PostgresSinkConfig{
		TargetHost: job.Sink.Host, TargetPort: job.Sink.Port, TargetDBName: job.Sink.Database,
		TargetUser: job.Sink.User, TargetPassword: job.Sink.Password,
		DisableForeignKeyChecks: job.Sink.DisableForeignKeys,
		ReplicationOrigin:       job.Sink.Origin,
	}
	var sink sinks.ManagedSink
	if job.Replay != nil {
		sink, err = sinks.NewReplaySink(connection, *job.Replay, job.Source.Group, job.Mapping)
	} else {
		sink, err = sinks.NewPostgresSinkWithConfig(connection)
	}
	if err != nil {
		return nil, nil, err
	}
	w := worker.NewWorker(natsClient, nil, nil, sink, job.Source.Group,
		worker.WithMapper(mapper),
		worker.WithBatchSize(job.Worker.BatchSize),
		worker.WithFlushInterval(job.FlushInterval()),
		worker.WithMaxDeliver(job.Worker.MaxDeliver),
	)
	return w, sink, nil
}

func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}

func runJobReplicator(cmd *cobra.Command, _ []string) error {
	job, err := loadJobFlag(cmd)
	if err != nil {
		return err
	}
	natsClient, err := createJobNATS(job)
	if err != nil {
		return err
	}
	defer natsClient.Close()
	rep, err := createJobReplicator(job, natsClient)
	if err != nil {
		return err
	}
	ctx, cancel := signalContext(cmd.Context())
	defer cancel()
	if err := rep.Start(ctx); err != nil && err != context.Canceled {
		return err
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	return rep.Stop(shutdownCtx)
}

func runJobWorker(cmd *cobra.Command, _ []string) error {
	job, err := loadJobFlag(cmd)
	if err != nil {
		return err
	}
	natsClient, err := createJobNATS(job)
	if err != nil {
		return err
	}
	defer natsClient.Close()
	w, sink, err := createJobWorker(job, natsClient)
	if err != nil {
		return err
	}
	defer sink.Close()
	ctx, cancel := signalContext(cmd.Context())
	defer cancel()
	return w.Start(ctx)
}

func runCompleteJob(cmd *cobra.Command, _ []string) error {
	job, err := loadJobFlag(cmd)
	if err != nil {
		return err
	}
	natsClient, err := createJobNATS(job)
	if err != nil {
		return err
	}
	defer natsClient.Close()
	w, sink, err := createJobWorker(job, natsClient)
	if err != nil {
		return err
	}
	defer sink.Close()
	rep, err := createJobReplicator(job, natsClient)
	if err != nil {
		return err
	}
	ctx, cancel := signalContext(cmd.Context())
	defer cancel()
	errCh := make(chan error, 2)
	go func() { errCh <- w.Start(ctx) }()
	go func() { errCh <- rep.Start(ctx) }()
	remaining := 2
	select {
	case <-ctx.Done():
	case err = <-errCh:
		remaining--
		if err == context.Canceled {
			err = nil
		}
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	for remaining > 0 {
		select {
		case stopped := <-errCh:
			remaining--
			if err == nil && stopped != context.Canceled {
				err = stopped
			}
		case <-shutdownCtx.Done():
			return fmt.Errorf("job did not finish shutdown: %w", shutdownCtx.Err())
		}
	}
	if stopErr := rep.Stop(shutdownCtx); stopErr != nil && err == nil {
		err = stopErr
	}
	return err
}

type previewDocument struct {
	Events []previewEvent `yaml:"events"`
}

type previewEvent struct {
	Operation     utils.OperationType    `yaml:"operation"`
	Schema        string                 `yaml:"schema"`
	Table         string                 `yaml:"table"`
	KeyColumns    []string               `yaml:"key-columns"`
	Before        map[string]interface{} `yaml:"before"`
	After         map[string]interface{} `yaml:"after"`
	Origin        string                 `yaml:"origin,omitempty"`
	TransactionID string                 `yaml:"transaction-id,omitempty"`
	LSN           string                 `yaml:"lsn,omitempty"`
}

func runJobPreview(cmd *cobra.Command, _ []string) error {
	job, err := loadJobFlag(cmd)
	if err != nil {
		return err
	}
	mapper, err := mapping.NewEngine(job.Mapping)
	if err != nil {
		return err
	}
	path, err := cmd.Flags().GetString("events")
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path) // #nosec G304 -- explicit operator-provided path
	if err != nil {
		return err
	}
	var document previewDocument
	if err := yaml.UnmarshalStrict(data, &document); err != nil {
		return fmt.Errorf("parse preview events: %w", err)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	for index, event := range document.Events {
		message := previewMessage(event, index)
		outputs, err := mapper.Map(message)
		if err != nil {
			return fmt.Errorf("event %d: %w", index+1, err)
		}
		for _, output := range outputs {
			view := map[string]interface{}{
				"transaction-id":   output.TransactionID,
				"operation":        output.Type,
				"target":           output.Schema + "." + output.Table,
				"key-columns":      output.ReplicationKey.Columns,
				"values":           output.Values,
				"old-values":       output.OldValues,
				"conflict":         output.ConflictAction,
				"json-patches":     output.JSONPatches,
				"require-existing": output.RequireExisting,
			}
			if err := encoder.Encode(view); err != nil {
				return err
			}
		}
	}
	return nil
}

func previewMessage(event previewEvent, index int) *utils.CDCMessage {
	nameSet := make(map[string]bool)
	for name := range event.Before {
		nameSet[name] = true
	}
	for name := range event.After {
		nameSet[name] = true
	}
	names := make([]string, 0, len(nameSet))
	for name := range nameSet {
		names = append(names, name)
	}
	sort.Strings(names)
	columns := make([]*pglogrepl.RelationMessageColumn, 0, len(names))
	for _, name := range names {
		columns = append(columns, &pglogrepl.RelationMessageColumn{Name: name})
	}
	transactionID := event.TransactionID
	if transactionID == "" {
		transactionID = fmt.Sprintf("preview-%d", index+1)
	}
	operation := utils.OperationType(strings.ToUpper(string(event.Operation)))
	return &utils.CDCMessage{
		Type: operation, Schema: event.Schema, Table: event.Table, Columns: columns,
		Values: event.After, OldValues: event.Before, Origin: event.Origin,
		TransactionID: transactionID, LSN: event.LSN,
		ReplicationKey: utils.ReplicationKey{Type: utils.ReplicationKeyPK, Columns: event.KeyColumns},
		EmittedAt:      time.Now(),
	}
}
