package collector

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/LoneWolf38/log-collector-system/goapp/internal/state"
)

const (
	parseChanSize = 8
	sendChanSize  = 16
)

type Config struct {
	InputDir      string
	BackupPath    string
	AggregatorURL string
	Workers       int
	BatchSize     int
	FlushInterval time.Duration
}

// Collector watches InputDir for ts_log_* files, manages one Batcher per file,
// and persists progress to BackupPath for crash recovery.
type Collector struct {
	UUID     string
	log      *slog.Logger
	cfg      Config
	stateMgr *state.Manager
	parseCh  chan string
	sendCh   chan LogEntry
	pool     *workerPool
	sender   *Sender
	mu       sync.Mutex
	batchers map[string]*Batcher
	stateMap map[string]state.BatcherState
	collUUID string
	batcherWg sync.WaitGroup
	globalWg  sync.WaitGroup
}

func New(log *slog.Logger, cfg Config) *Collector {
	parseCh := make(chan string, parseChanSize)
	sendCh := make(chan LogEntry, sendChanSize)
	c := &Collector{
		UUID:     uuid.New().String(),
		log:      log,
		cfg:      cfg,
		stateMgr: state.NewManager(cfg.BackupPath),
		parseCh:  parseCh,
		sendCh:   sendCh,
		batchers: make(map[string]*Batcher),
		stateMap: make(map[string]state.BatcherState),
	}
	c.pool = newWorkerPool(log, cfg.Workers, parseCh, sendCh)
	c.sender = NewSender(log, cfg.AggregatorURL, cfg.BatchSize, cfg.FlushInterval, sendCh)
	return c
}

func (c *Collector) Run(ctx context.Context) error {
	if err := os.MkdirAll(c.cfg.InputDir, 0o755); err != nil {
		return err
	}
	if err := c.loadAndRestore(ctx); err != nil {
		c.log.Error("restore failed, starting fresh", "error", err)
		c.collUUID = uuid.New().String()
	}
	if err := c.seedNew(ctx); err != nil {
		return err
	}
	c.globalWg.Add(2)
	go func() { defer c.globalWg.Done(); c.pool.run(ctx) }()
	go func() { defer c.globalWg.Done(); c.sender.Run(ctx) }()

	c.log.Info("watching", "dir", c.cfg.InputDir)
	c.pollLoop(ctx)

	c.mu.Lock()
	for _, b := range c.batchers {
		b.Signal()
	}
	c.mu.Unlock()
	c.batcherWg.Wait()
	c.saveState()
	close(c.parseCh)
	c.globalWg.Wait()
	return nil
}

func (c *Collector) pollLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

func (c *Collector) reconcile(ctx context.Context) {
	entries, err := os.ReadDir(c.cfg.InputDir)
	if err != nil {
		c.log.Error("readdir", "error", err)
		return
	}
	current := make(map[string]struct{})
	for _, e := range entries {
		if !e.IsDir() && isLogFile(e.Name()) {
			current[filepath.Join(c.cfg.InputDir, e.Name())] = struct{}{}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for path := range current {
		if _, exists := c.batchers[path]; !exists {
			c.spawnBatcher(ctx, path, nil)
		}
	}
	for path, b := range c.batchers {
		if _, exists := current[path]; !exists {
			b.Signal()
			delete(c.batchers, path)
			c.log.Info("file rotated", "file", path)
		}
	}
}

func (c *Collector) spawnBatcher(ctx context.Context, path string, saved *state.BatcherState) {
	onSave := func(bs state.BatcherState) {
		c.mu.Lock()
		c.stateMap[bs.File] = bs
		c.mu.Unlock()
		c.saveState()
	}
	var b *Batcher
	if saved != nil {
		b = NewBatcherFromState(c.log, *saved, c.parseCh, onSave)
	} else {
		b = NewBatcher(c.log, path, c.parseCh, onSave)
	}
	c.batchers[path] = b
	c.batcherWg.Add(1)
	go func() {
		defer c.batcherWg.Done()
		b.Run(ctx)
	}()
	c.log.Info("batcher started", "file", path, "uuid", b.UUID)
}

func (c *Collector) saveState() {
	c.mu.Lock()
	batchers := make([]state.BatcherState, 0, len(c.stateMap))
	for _, bs := range c.stateMap {
		batchers = append(batchers, bs)
	}
	collUUID := c.collUUID
	c.mu.Unlock()
	root := &state.Root{Collector: state.CollectorState{UUID: collUUID, Batchers: batchers}}
	if err := c.stateMgr.Save(root); err != nil {
		c.log.Error("save state failed", "error", err)
	}
}

func (c *Collector) loadAndRestore(ctx context.Context) error {
	root, err := c.stateMgr.Load()
	if err != nil {
		return err
	}
	if root == nil {
		c.collUUID = uuid.New().String()
		return nil
	}
	c.collUUID = root.Collector.UUID
	c.log.Info("restoring", "batchers", len(root.Collector.Batchers))
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, bs := range root.Collector.Batchers {
		saved := bs
		c.stateMap[bs.File] = saved
		if _, err := os.Stat(bs.File); os.IsNotExist(err) {
			c.log.Warn("saved file gone, skipping", "file", bs.File)
			continue
		}
		c.spawnBatcher(ctx, bs.File, &saved)
	}
	return nil
}

func (c *Collector) seedNew(ctx context.Context) error {
	entries, err := os.ReadDir(c.cfg.InputDir)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range entries {
		if e.IsDir() || !isLogFile(e.Name()) {
			continue
		}
		path := filepath.Join(c.cfg.InputDir, e.Name())
		if _, exists := c.batchers[path]; !exists {
			c.spawnBatcher(ctx, path, nil)
		}
	}
	return nil
}

func isLogFile(name string) bool {
	return strings.HasPrefix(filepath.Base(name), "ts_log_")
}

// ── WorkerPool ────────────────────────────────────────────────────────────────

type workerPool struct {
	log     *slog.Logger
	n       int
	parseCh <-chan string
	sendCh  chan<- LogEntry
	wg      sync.WaitGroup
}

func newWorkerPool(log *slog.Logger, n int, parseCh <-chan string, sendCh chan<- LogEntry) *workerPool {
	return &workerPool{log: log, n: n, parseCh: parseCh, sendCh: sendCh}
}

func (p *workerPool) run(ctx context.Context) {
	for i := range p.n {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			for {
				select {
				case line, ok := <-p.parseCh:
					if !ok {
						return
					}
					entry, err := parseLine(line)
					if err != nil {
						p.log.Warn("parse error", "worker_id", id, "error", err)
						continue
					}
					select {
					case p.sendCh <- entry:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}
	p.wg.Wait()
}
