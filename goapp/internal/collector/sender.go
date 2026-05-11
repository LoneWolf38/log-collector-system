package collector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// LogEntry is the output format: {"id":"...","time":"...","data":"..."}
type LogEntry struct {
	ID   string `json:"id"`
	Time string `json:"time"`
	Data string `json:"data"`
}

func parseLine(line string) (LogEntry, error) {
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return LogEntry{}, fmt.Errorf("expected 3 space-separated fields, got %d", len(parts))
	}
	ts, id, data := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])
	if ts == "" || id == "" || data == "" {
		return LogEntry{}, fmt.Errorf("empty field in line")
	}
	return LogEntry{ID: id, Time: ts, Data: data}, nil
}

// Sender batches LogEntries and POSTs them to the aggregator as JSON arrays.
type Sender struct {
	log           *slog.Logger
	aggregatorURL string
	batchSize     int
	flushInterval time.Duration
	in            <-chan LogEntry
	client        *http.Client
}

func NewSender(log *slog.Logger, url string, batchSize int, flushInterval time.Duration, in <-chan LogEntry) *Sender {
	return &Sender{
		log:           log,
		aggregatorURL: url,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		in:            in,
		// No global timeout on the client; each POST sets its own via postWithRetry.
		client: &http.Client{},
	}
}

func (s *Sender) Run(ctx context.Context) {
	batch := make([]LogEntry, 0, s.batchSize)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	flush := func(reason string) {
		if len(batch) == 0 {
			return
		}
		s.log.Info("flushing", "reason", reason, "count", len(batch))
		if err := s.postWithRetry(ctx, batch); err != nil {
			s.log.Error("POST failed after retries, dropping batch", "error", err, "dropped", len(batch))
		}
		batch = batch[:0]
	}

	for {
		select {
		case entry, ok := <-s.in:
			if !ok {
				flush("closed")
				return
			}
			batch = append(batch, entry)
			if len(batch) >= s.batchSize {
				flush("batch_full")
			}
		case <-ticker.C:
			flush("interval")
		case <-ctx.Done():
			flush("shutdown")
			return
		}
	}
}

const (
	maxPostAttempts = 5
	maxBackoff      = 30 * time.Second
	// postTimeout must exceed batchSize × aggregator processing time per entry.
	// The spec states ~30 s/entry; at the default batch size of 50 that is 25 min.
	postTimeout = 30 * time.Minute
)

// postWithRetry retries the POST with exponential backoff, respecting ctx for
// clean shutdown between attempts.
func (s *Sender) postWithRetry(ctx context.Context, batch []LogEntry) error {
	backoff := time.Second
	for attempt := 1; attempt <= maxPostAttempts; attempt++ {
		err := s.post(batch)
		if err == nil {
			return nil
		}
		if attempt == maxPostAttempts {
			return fmt.Errorf("attempt %d/%d: %w", attempt, maxPostAttempts, err)
		}
		s.log.Warn("POST failed, retrying", "attempt", attempt, "backoff", backoff, "error", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	return nil
}

func (s *Sender) post(batch []LogEntry) error {
	body, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), postTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.aggregatorURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("aggregator returned %d", resp.StatusCode)
	}
	return nil
}
