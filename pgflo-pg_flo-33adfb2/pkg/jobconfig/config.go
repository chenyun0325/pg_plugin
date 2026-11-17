// Package jobconfig loads the unified pgflo/v1 YAML configuration.
package jobconfig

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pgflo/pg_flo/pkg/mapping"
	"github.com/pgflo/pg_flo/pkg/replicator"
	"github.com/pgflo/pg_flo/pkg/utils"
	"gopkg.in/yaml.v2"
)

type Job struct {
	APIVersion string         `yaml:"api-version" json:"api-version"`
	Kind       string         `yaml:"kind" json:"kind"`
	Metadata   Metadata       `yaml:"metadata" json:"metadata"`
	Source     Source         `yaml:"source" json:"source"`
	NATS       NATS           `yaml:"nats" json:"nats"`
	Sink       Sink           `yaml:"sink" json:"sink"`
	Worker     Worker         `yaml:"worker" json:"worker"`
	Mapping    mapping.Config `yaml:"mapping" json:"mapping"`
}

type Metadata struct {
	Name string `yaml:"name" json:"name"`
}

type Source struct {
	Type                     string   `yaml:"type" json:"type"`
	ID                       string   `yaml:"id" json:"id"`
	Host                     string   `yaml:"host" json:"host"`
	Port                     int      `yaml:"port" json:"port"`
	Database                 string   `yaml:"database" json:"database"`
	User                     string   `yaml:"user" json:"user"`
	Password                 string   `yaml:"password" json:"-"`
	Schema                   string   `yaml:"schema" json:"schema"`
	Tables                   []string `yaml:"tables" json:"tables"`
	Group                    string   `yaml:"group" json:"group"`
	Mode                     string   `yaml:"mode" json:"mode"`
	MaxCopyWorkers           int      `yaml:"max-copy-workers" json:"max-copy-workers"`
	TrackDDL                 bool     `yaml:"track-ddl" json:"track-ddl"`
	FilterReplicationOrigins bool     `yaml:"filter-replication-origins" json:"filter-replication-origins"`
}

type NATS struct {
	URL      string `yaml:"url" json:"url"`
	MaxAge   string `yaml:"max-age" json:"max-age"`
	Replicas int    `yaml:"replicas" json:"replicas"`
}

type Sink struct {
	Type               string `yaml:"type" json:"type"`
	Host               string `yaml:"host" json:"host"`
	Port               int    `yaml:"port" json:"port"`
	Database           string `yaml:"database" json:"database"`
	User               string `yaml:"user" json:"user"`
	Password           string `yaml:"password" json:"-"`
	Origin             string `yaml:"replication-origin" json:"replication-origin"`
	DisableForeignKeys bool   `yaml:"disable-foreign-keys" json:"disable-foreign-keys"`
}

type Worker struct {
	BatchSize     int    `yaml:"batch-size" json:"batch-size"`
	FlushInterval string `yaml:"flush-interval" json:"flush-interval"`
	MaxDeliver    int    `yaml:"max-deliver" json:"max-deliver"`
}

func Load(path string) (*Job, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- explicit operator-provided path
	if err != nil {
		return nil, fmt.Errorf("read job YAML: %w", err)
	}
	expanded, err := expandEnvironment(string(data))
	if err != nil {
		return nil, err
	}
	var job Job
	if err := yaml.UnmarshalStrict([]byte(expanded), &job); err != nil {
		return nil, fmt.Errorf("parse job YAML: %w", err)
	}
	job.setDefaults()
	if err := job.Validate(); err != nil {
		return nil, err
	}
	return &job, nil
}

func (j *Job) setDefaults() {
	if j.APIVersion == "" {
		j.APIVersion = "pgflo/v1"
	}
	if j.Kind == "" {
		j.Kind = "ReplicationJob"
	}
	if j.Source.Type == "" {
		j.Source.Type = "postgres"
	}
	if j.Source.Port == 0 {
		j.Source.Port = 5432
	}
	if j.Source.Schema == "" {
		j.Source.Schema = "public"
	}
	if j.Source.Group == "" {
		j.Source.Group = j.Metadata.Name
	}
	if j.Source.Mode == "" {
		j.Source.Mode = "stream"
	}
	if j.Source.MaxCopyWorkers == 0 {
		j.Source.MaxCopyWorkers = 4
	}
	if j.NATS.MaxAge == "" {
		j.NATS.MaxAge = "0s"
	}
	if j.NATS.Replicas == 0 {
		j.NATS.Replicas = 1
	}
	if j.Sink.Type == "" {
		j.Sink.Type = "postgres"
	}
	if j.Sink.Port == 0 {
		j.Sink.Port = 5432
	}
	if j.Worker.BatchSize == 0 {
		j.Worker.BatchSize = 1000
	}
	if j.Worker.FlushInterval == "" {
		j.Worker.FlushInterval = "500ms"
	}
	if j.Worker.MaxDeliver == 0 {
		j.Worker.MaxDeliver = 20
	}
	if j.Mapping.SourceID == "" {
		j.Mapping.SourceID = j.Source.ID
	}
	if j.Mapping.Version == "" {
		j.Mapping.Version = "v1"
	}
}

func (j *Job) Validate() error {
	if j.APIVersion != "pgflo/v1" || j.Kind != "ReplicationJob" {
		return fmt.Errorf("api-version must be pgflo/v1 and kind must be ReplicationJob")
	}
	if j.Metadata.Name == "" {
		return fmt.Errorf("metadata.name is required")
	}
	if j.Source.Type != "postgres" || j.Sink.Type != "postgres" {
		return fmt.Errorf("this implementation supports postgres source and sink")
	}
	if j.Source.ID == "" || j.Source.Host == "" || j.Source.Database == "" || j.Source.User == "" || j.Source.Group == "" {
		return fmt.Errorf("source id, host, database, user, and group are required")
	}
	if j.Source.Port < 1 || j.Source.Port > 65535 || j.Sink.Port < 1 || j.Sink.Port > 65535 {
		return fmt.Errorf("source/sink port is outside 1..65535")
	}
	if len(j.Source.Tables) == 0 {
		return fmt.Errorf("source.tables must not be empty")
	}
	switch j.Source.Mode {
	case "stream", "copy", "copy-and-stream":
	default:
		return fmt.Errorf("source.mode must be stream, copy, or copy-and-stream")
	}
	if j.NATS.URL == "" {
		return fmt.Errorf("nats.url is required")
	}
	if _, err := time.ParseDuration(j.NATS.MaxAge); err != nil {
		return fmt.Errorf("nats.max-age: %w", err)
	}
	if j.NATS.Replicas < 1 || j.NATS.Replicas > 5 {
		return fmt.Errorf("nats.replicas must be between 1 and 5")
	}
	if j.Sink.Host == "" || j.Sink.Database == "" || j.Sink.User == "" {
		return fmt.Errorf("sink host, database, and user are required")
	}
	if j.Mapping.LoopProtection && (!j.Source.FilterReplicationOrigins || j.Sink.Origin == "") {
		return fmt.Errorf("loop protection requires source.filter-replication-origins=true and sink.replication-origin")
	}
	if j.Worker.BatchSize < 1 || j.Worker.MaxDeliver < 1 {
		return fmt.Errorf("worker batch-size and max-deliver must be positive")
	}
	if _, err := time.ParseDuration(j.Worker.FlushInterval); err != nil {
		return fmt.Errorf("worker.flush-interval: %w", err)
	}
	// NewEngine performs the mapping-specific strict validation.
	if _, err := mapping.NewEngine(j.Mapping); err != nil {
		return fmt.Errorf("mapping: %w", err)
	}
	return nil
}

func (j *Job) ReplicatorConfig() replicator.Config {
	return replicator.Config{
		Host:                     j.Source.Host,
		Port:                     uint16(j.Source.Port),
		Database:                 j.Source.Database,
		User:                     j.Source.User,
		Password:                 j.Source.Password,
		Group:                    j.Source.Group,
		Schema:                   j.Source.Schema,
		Tables:                   append([]string(nil), j.Source.Tables...),
		TrackDDL:                 j.Source.TrackDDL,
		FilterReplicationOrigins: j.Source.FilterReplicationOrigins,
		MappingVersion:           j.Mapping.Version,
		RetryConfig: utils.RetryConfig{
			MaxAttempts: 3,
			InitialWait: time.Second,
			MaxWait:     8 * time.Second,
		},
	}
}

func (j *Job) NATSMaxAge() time.Duration {
	duration, _ := time.ParseDuration(j.NATS.MaxAge)
	return duration
}

func (j *Job) FlushInterval() time.Duration {
	duration, _ := time.ParseDuration(j.Worker.FlushInterval)
	return duration
}

var environmentPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

func expandEnvironment(input string) (string, error) {
	var expansionErr error
	result := environmentPattern.ReplaceAllStringFunc(input, func(token string) string {
		matches := environmentPattern.FindStringSubmatch(token)
		if value, ok := os.LookupEnv(matches[1]); ok {
			return value
		}
		if matches[2] != "" {
			return matches[3]
		}
		expansionErr = fmt.Errorf("environment variable %s referenced by job YAML is not set", matches[1])
		return token
	})
	return result, expansionErr
}

// RedactedSummary is safe to print in validate output.
func (j *Job) RedactedSummary() string {
	return strings.Join([]string{
		"job=" + j.Metadata.Name,
		"source=" + j.Source.ID + "@" + j.Source.Host + ":" + strconv.Itoa(j.Source.Port) + "/" + j.Source.Database,
		"group=" + j.Source.Group,
		"mode=" + j.Source.Mode,
		"sink=" + j.Sink.Host + ":" + strconv.Itoa(j.Sink.Port) + "/" + j.Sink.Database,
		"routes=" + strconv.Itoa(len(j.Mapping.Routes)),
	}, " ")
}
