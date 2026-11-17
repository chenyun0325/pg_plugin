// Package worker provides message processing workers for pg_flo.
package worker

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go" // Use the standard NATS package
	"github.com/pgflo/pg_flo/pkg/pgflonats"
	"github.com/pgflo/pg_flo/pkg/routing"
	"github.com/pgflo/pg_flo/pkg/rules"
	"github.com/pgflo/pg_flo/pkg/sinks"
	"github.com/pgflo/pg_flo/pkg/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Worker represents a worker that processes messages from NATS.
type Worker struct {
	natsClient     *pgflonats.NATSClient
	ruleEngine     *rules.RuleEngine
	router         *routing.Router
	sink           sinks.Sink
	group          string
	logger         utils.Logger
	batchSize      int
	buffer         []*utils.CDCMessage
	ackBuffer      []*nats.Msg
	lastSavedState uint64
	flushInterval  time.Duration
	maxDeliver     int
	mapper         Mapper
	shutdownCh     chan struct{}
	wg             sync.WaitGroup
}

// Mapper expands one source row into zero or more target rows.
type Mapper interface {
	Map(*utils.CDCMessage) ([]*utils.CDCMessage, error)
}

// Option is a function type that modifies Worker configuration
type Option func(*Worker)

// WithBatchSize sets the batch size for the worker
func WithBatchSize(size int) Option {
	return func(w *Worker) {
		w.batchSize = size
	}
}

func WithFlushInterval(interval time.Duration) Option {
	return func(w *Worker) {
		if interval > 0 {
			w.flushInterval = interval
		}
	}
}

func WithMaxDeliver(maxDeliver int) Option {
	return func(w *Worker) {
		if maxDeliver > 0 {
			w.maxDeliver = maxDeliver
		}
	}
}

func WithMapper(mapper Mapper) Option {
	return func(w *Worker) { w.mapper = mapper }
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05.000"})
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000Z07:00"
}

// NewWorker creates and returns a new Worker instance with the provided NATS client
func NewWorker(natsClient *pgflonats.NATSClient, ruleEngine *rules.RuleEngine, router *routing.Router, sink sinks.Sink, group string, opts ...Option) *Worker {
	logger := utils.NewZerologLogger(log.With().Str("component", "worker").Logger())

	w := &Worker{
		natsClient:     natsClient,
		ruleEngine:     ruleEngine,
		router:         router,
		sink:           sink,
		group:          group,
		logger:         logger,
		batchSize:      1000,
		buffer:         make([]*utils.CDCMessage, 0, 1000),
		ackBuffer:      make([]*nats.Msg, 0, 1000),
		lastSavedState: 0,
		flushInterval:  500 * time.Millisecond,
		maxDeliver:     20,
		shutdownCh:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}
	w.buffer = make([]*utils.CDCMessage, 0, w.batchSize)
	w.ackBuffer = make([]*nats.Msg, 0, w.batchSize)

	return w
}

// Start begins the worker's message processing loop, setting up the NATS consumer and processing messages.
func (w *Worker) Start(ctx context.Context) error {
	stream := fmt.Sprintf("pgflo_%s_stream", w.group)
	subject := fmt.Sprintf("pgflo.%s", w.group)

	w.logger.Info().
		Str("stream", stream).
		Str("subject", subject).
		Str("group", w.group).
		Msg("Starting worker")

	js := w.natsClient.JetStream()

	consumerName := fmt.Sprintf("pgflo_%s_consumer", w.group)

	consumerConfig := &nats.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: subject,
		AckPolicy:     nats.AckExplicitPolicy,
		MaxDeliver:    w.maxDeliver,
		AckWait:       25 * time.Minute,
		DeliverPolicy: nats.DeliverAllPolicy,
	}

	_, err := js.AddConsumer(stream, consumerConfig)
	if err != nil && !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
		w.logger.Error().Err(err).Msg("Failed to add or update consumer")
		return fmt.Errorf("failed to add or update consumer: %w", err)
	}

	sub, err := js.PullSubscribe(subject, consumerName)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to subscribe to subject")
		return fmt.Errorf("failed to subscribe to subject: %w", err)
	}

	errCh := make(chan error, 1)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.processMessages(ctx, sub); err != nil && err != context.Canceled {
			w.logger.Error().Err(err).Msg("Error processing messages")
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}
	w.logger.Info().Msg("Received shutdown signal. Initiating graceful shutdown...")

	w.wg.Wait()
	w.logger.Debug().Msg("All goroutines finished")

	return w.flushBuffer()
}

// processMessages continuously processes messages from the NATS consumer.
func (w *Worker) processMessages(ctx context.Context, sub *nats.Subscription) error {
	flushTicker := time.NewTicker(w.flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info().Msg("Flushing remaining messages")
			return w.flushBuffer()
		case <-flushTicker.C:
			if err := w.flushBuffer(); err != nil {
				w.logger.Error().Err(err).Msg("Failed to flush buffer on interval")
				return err
			}
		default:
			msgs, err := sub.Fetch(w.batchSize, nats.MaxWait(1*time.Second))
			if err != nil && !errors.Is(err, nats.ErrTimeout) {
				w.logger.Error().Err(err).Msg("Error fetching messages")
				continue
			}

			for _, msg := range msgs {
				if err := w.processMessage(msg); err != nil {
					w.logger.Error().Err(err).Msg("Failed to process message")
					if nakErr := msg.NakWithDelay(5 * time.Second); nakErr != nil {
						w.logger.Error().Err(nakErr).Msg("Failed to NAK failed message")
					}
					return fmt.Errorf("ordered stream stopped on failed message: %w", err)
				}
			}
			if len(w.buffer) >= w.batchSize {
				if err := w.flushBuffer(); err != nil {
					w.logger.Error().Err(err).Msg("Failed to flush buffer")
					return err
				}
			}
		}
	}
}

// processMessage handles a single message, applying rules, writing to the sink, and updating the last processed sequence.
func (w *Worker) processMessage(msg *nats.Msg) error {
	metadata, err := msg.Metadata()
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get message metadata")
		return err
	}

	envelope, err := decodeEnvelope(msg.Data)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to unmarshal message")
		return err
	}
	var mapped []*utils.CDCMessage
	for i := range envelope.Messages {
		cdcMessage := envelope.Messages[i]
		cdcMessage.TransactionID = envelope.TransactionID
		if envelope.MappingVersion != "" {
			cdcMessage.MappingVersion = envelope.MappingVersion
		}
		if envelope.CommitLSN != "" {
			cdcMessage.LSN = envelope.CommitLSN
		}
		outputs, err := w.processCDCMessage(&cdcMessage)
		if err != nil {
			return fmt.Errorf("transaction %s: %w", envelope.TransactionID, err)
		}
		mapped = append(mapped, outputs...)
	}

	w.lastSavedState = metadata.Sequence.Stream
	if len(mapped) == 0 {
		if err := w.saveState(); err != nil {
			return err
		}
		return msg.Ack()
	}
	w.ackBuffer = append(w.ackBuffer, msg)
	w.buffer = append(w.buffer, mapped...)

	return nil
}

func decodeEnvelope(data []byte) (utils.CDCEnvelope, error) {
	var envelope utils.CDCEnvelope
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&envelope); err == nil && envelope.Version > 0 {
		if len(envelope.Messages) == 0 {
			return envelope, fmt.Errorf("empty CDC envelope")
		}
		return envelope, nil
	}
	// Backward compatibility for messages already present in a legacy stream.
	var message utils.CDCMessage
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&message); err != nil {
		return envelope, err
	}
	return utils.CDCEnvelope{Version: 1, TransactionID: message.TransactionID, CommitLSN: message.LSN, Messages: []utils.CDCMessage{message}}, nil
}

func (w *Worker) processCDCMessage(message *utils.CDCMessage) ([]*utils.CDCMessage, error) {
	if w.ruleEngine != nil {
		processed, err := w.ruleEngine.ApplyRules(message)
		if err != nil {
			return nil, fmt.Errorf("apply legacy rules: %w", err)
		}
		if processed == nil {
			return nil, nil
		}
		message = processed
	}
	if w.router != nil {
		routed, err := w.router.ApplyRouting(message)
		if err != nil {
			return nil, fmt.Errorf("apply legacy routing: %w", err)
		}
		if routed == nil {
			return nil, nil
		}
		message = routed
	}
	if w.mapper != nil {
		return w.mapper.Map(message)
	}
	return []*utils.CDCMessage{message}, nil
}

// flushBuffer writes the buffered messages to the sink and updates the last processed sequence.
func (w *Worker) flushBuffer() error {
	if len(w.buffer) == 0 {
		return nil
	}

	w.logger.Debug().
		Int("messages", len(w.buffer)).
		Int("batch_size", w.batchSize).
		Msg("Flushing buffer")

	// Write to sink first - if this fails, messages remain unacked for redelivery
	err := w.sink.WriteBatch(w.buffer)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to write batch to sink")
		return err
	}

	// Store the checkpoint before ACK. If this fails, the already committed
	// target transaction is replayed and the mapped upsert/delete is idempotent.
	if err := w.saveState(); err != nil {
		return err
	}
	for _, msg := range w.ackBuffer {
		if ackErr := msg.Ack(); ackErr != nil {
			w.logger.Error().Err(ackErr).Msg("Failed to acknowledge message after successful write")
		}
	}

	w.buffer = w.buffer[:0]
	w.ackBuffer = w.ackBuffer[:0]
	return nil
}

func (w *Worker) saveState() error {
	state, err := w.natsClient.GetState()
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to get current state")
		return err
	}
	state.LastProcessedSeq[w.group] = w.lastSavedState
	if err := w.natsClient.SaveState(state); err != nil {
		w.logger.Error().Err(err).Msg("Failed to save state")
		return err
	}
	return nil
}
