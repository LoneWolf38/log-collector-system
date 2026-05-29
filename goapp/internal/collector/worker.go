package collector

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/LoneWolf38/log-collector-system/goapp/internal/state"
)

// Worker reads log lines from a single file starting at StartingOffset,
// advancing CurrentOffset as each line is consumed.
//
// StartingOffset is fixed at creation and marks where this Worker's reading
// session began. CurrentOffset is the live watermark — the restore state
// snapshots it so the Worker can resume exactly here after a crash.
//
// Multiple Workers can exist per Batcher when segments of the same file are
// processed in parallel (e.g. a large completed file split across N goroutines).
// In the common case of an actively-growing file there is one Worker per Batcher.
type Worker struct {
	UUID           string
	StartingOffset int64
	currentOffset  atomic.Int64 // written by the read goroutine, read by Batcher for state

	log     *slog.Logger
	file    string
	parseCh chan<- string // capped — backpressure up to the file reader
}

// NewWorker creates a Worker that starts reading at startingOffset.
// If restoring from a crash, pass the saved CurrentOffset as startingOffset
// so the Worker resumes without re-processing already-sent lines.
func NewWorker(log *slog.Logger, file string, startingOffset int64, parseCh chan<- string) *Worker {
	w := &Worker{
		UUID:           uuid.New().String(),
		StartingOffset: startingOffset,
		log:            log,
		file:           file,
		parseCh:        parseCh,
	}
	w.currentOffset.Store(startingOffset)
	return w
}

// NewWorkerFromState reconstructs a Worker from a saved WorkerState.
// The Worker resumes from state.CurrentOffset, preserving the original UUID.
func NewWorkerFromState(log *slog.Logger, file string, ws state.WorkerState, parseCh chan<- string) *Worker {
	w := &Worker{
		UUID:           ws.UUID,
		StartingOffset: ws.StartingOffset,
		log:            log,
		file:           file,
		parseCh:        parseCh,
	}
	w.currentOffset.Store(ws.CurrentOffset)
	return w
}

// CurrentOffset returns the current read watermark. Thread-safe.
func (w *Worker) CurrentOffset() int64 { return w.currentOffset.Load() }

// State returns a snapshot of this Worker suitable for persistence.
func (w *Worker) State() state.WorkerState {
	return state.WorkerState{
		UUID:           w.UUID,
		Timestamp:      time.Now().UTC(),
		StartingOffset: w.StartingOffset,
		CurrentOffset:  w.CurrentOffset(),
	}
}

// Run reads from the file at CurrentOffset, sending each line to parseCh.
// It blocks until ctx is cancelled or done is closed (file rotated away).
// The done channel is closed by the Batcher when the file is removed.
func (w *Worker) Run(ctx context.Context, done <-chan struct{}) {
	w.log.Info("worker: starting",
		"uuid", w.UUID, "file", w.file,
		"starting_offset", w.StartingOffset,
		"resume_offset", w.CurrentOffset())

	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()

	for {
		consumed, err := w.readAvailable(ctx)
		if consumed > 0 {
			w.currentOffset.Add(consumed)
		}
		if err != nil && err != io.EOF {
			w.log.Error("worker: read error", "uuid", w.UUID, "file", w.file, "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-done:
			// File rotated — one final drain to capture any tail bytes.
			consumed, _ := w.readAvailable(ctx)
			if consumed > 0 {
				w.currentOffset.Add(consumed)
			}
			w.log.Info("worker: file rotated, final drain done",
				"uuid", w.UUID, "file", w.file, "final_offset", w.CurrentOffset())
			return
		case <-poll.C:
		}
	}
}

// readAvailable opens the file, seeks to CurrentOffset, and reads all complete
// lines available, forwarding each to parseCh. Returns bytes consumed.
func (w *Worker) readAvailable(ctx context.Context) (int64, error) {
	f, err := os.Open(w.file)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(w.currentOffset.Load(), io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek: %w", err)
	}

	const maxLine = 110 << 20 // 110 MiB — accommodates max spec line size
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64<<10), maxLine)

	var consumed int64
	for scanner.Scan() {
		line := scanner.Text()
		consumed += int64(len(line)) + 1 // +1 for newline

		if strings.TrimSpace(line) == "" {
			continue
		}

		// Blocking send into the capped channel — natural backpressure.
		select {
		case w.parseCh <- line:
		case <-ctx.Done():
			return consumed, ctx.Err()
		}
	}
	return consumed, scanner.Err()
}
