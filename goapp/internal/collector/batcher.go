package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/LoneWolf38/log-collector-system/goapp/internal/state"
)

// Batcher manages all Workers for a single log file and periodically flushes
// their progress to the restore state via the onSave callback.
//
// On first encounter of a file, Batcher creates one Worker starting at offset 0.
// On restore, it recreates Workers from their saved WorkerState entries so each
// resumes from its exact CurrentOffset — no lines are re-processed or skipped.
//
// The onSave callback is called after every state flush interval and on shutdown.
// The Collector provides this callback and handles the actual disk write.
type Batcher struct {
	UUID    string
	file    string
	onSave  func(state.BatcherState) // called by Batcher to push state up to Collector
	parseCh chan<- string             // shared capped channel → WorkerPool

	log     *slog.Logger
	workers []*Worker
	mu      sync.Mutex

	done chan struct{} // closed by Collector on file rotation
}

func NewBatcher(log *slog.Logger, file string, parseCh chan<- string, onSave func(state.BatcherState)) *Batcher {
	return &Batcher{
		UUID:    uuid.New().String(),
		file:    file,
		parseCh: parseCh,
		onSave:  onSave,
		log:     log,
		done:    make(chan struct{}),
	}
}

// NewBatcherFromState reconstructs a Batcher from a saved BatcherState,
// preserving the UUID and recreating each saved Worker at its CurrentOffset.
func NewBatcherFromState(log *slog.Logger, bs state.BatcherState, parseCh chan<- string, onSave func(state.BatcherState)) *Batcher {
	b := &Batcher{
		UUID:    bs.UUID,
		file:    bs.File,
		parseCh: parseCh,
		onSave:  onSave,
		log:     log,
		done:    make(chan struct{}),
	}
	for _, ws := range bs.Workers {
		b.workers = append(b.workers, NewWorkerFromState(log, bs.File, ws, parseCh))
	}
	return b
}

// Signal is called by the Collector when the file has been removed (rotation).
func (b *Batcher) Signal() {
	select {
	case <-b.done:
	default:
		close(b.done)
	}
}

// State returns a snapshot of this Batcher and all its Workers for persistence.
func (b *Batcher) State() state.BatcherState {
	b.mu.Lock()
	defer b.mu.Unlock()

	ws := make([]state.WorkerState, 0, len(b.workers))
	for _, w := range b.workers {
		ws = append(ws, w.State())
	}
	return state.BatcherState{
		UUID:      b.UUID,
		File:      b.file,
		Workers:   ws,
		Timestamp: time.Now().UTC(),
	}
}

// Run starts workers (or creates a fresh one for a new file) and blocks until
// the context is cancelled or the file is rotated away. State is flushed to
// the Collector every flushInterval and once more on shutdown.
func (b *Batcher) Run(ctx context.Context) {
	b.mu.Lock()
	if len(b.workers) == 0 {
		// Fresh file: start one Worker at offset 0.
		b.workers = append(b.workers, NewWorker(b.log, b.file, 0, b.parseCh))
	}
	workers := make([]*Worker, len(b.workers))
	copy(workers, b.workers)
	b.mu.Unlock()

	b.log.Info("batcher: starting", "uuid", b.UUID, "file", b.file, "workers", len(workers))

	// Run all workers concurrently. Each owns its byte range.
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(wk *Worker) {
			defer wg.Done()
			wk.Run(ctx, b.done)
		}(w)
	}

	// Periodically flush state to the Collector.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	flush := func() { b.onSave(b.State()) }

	for {
		select {
		case <-ticker.C:
			flush()
		case <-b.done:
			// File rotated — workers will drain and exit on their own.
			wg.Wait()
			flush() // final state snapshot after all workers have finished
			b.log.Info("batcher: rotation complete", "uuid", b.UUID, "file", b.file)
			return
		case <-ctx.Done():
			wg.Wait()
			flush()
			return
		}
	}
}
