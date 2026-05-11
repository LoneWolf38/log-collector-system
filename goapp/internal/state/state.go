package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ── Types ─────────────────────────────────────────────────────────────────────

type WorkerState struct {
	UUID           string    `json:"uuid"`
	Timestamp      time.Time `json:"timestamp"`
	StartingOffset int64     `json:"startingOffset"`
	CurrentOffset  int64     `json:"currentOffset"`
}

type BatcherState struct {
	UUID      string        `json:"uuid"`
	File      string        `json:"file,omitempty"`
	Workers   []WorkerState `json:"workers"`
	Checksum  string        `json:"checksum"`
	Timestamp time.Time     `json:"timestamp,omitempty"`
}

type CollectorState struct {
	UUID      string         `json:"uuid"`
	Batchers  []BatcherState `json:"batchers"`
	Checksum  string         `json:"checksum"`
	Timestamp time.Time      `json:"timestamp"`
}

type Root struct {
	Collector CollectorState `json:"collector"`
}

// ── Manager ───────────────────────────────────────────────────────────────────

// Manager handles atomic persistence and checksum-verified loading of the
// restore file. Writes are tmp → rename so a crash mid-write leaves the
// previous file intact.
type Manager struct {
	path string
	mu   sync.Mutex
}

func NewManager(path string) *Manager {
	return &Manager{path: path}
}

func (m *Manager) Load() (*Root, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := os.ReadFile(m.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", m.path, err)
	}
	var root Root
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse %s: %w", m.path, err)
	}
	if err := verifyCollector(&root.Collector); err != nil {
		return nil, fmt.Errorf("collector checksum invalid: %w", err)
	}
	for i := range root.Collector.Batchers {
		if err := verifyBatcher(&root.Collector.Batchers[i]); err != nil {
			return nil, fmt.Errorf("batcher[%d] checksum invalid: %w", i, err)
		}
	}
	return &root, nil
}

func (m *Manager) Save(root *Root) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	root.Collector.Timestamp = now
	for i := range root.Collector.Batchers {
		b := &root.Collector.Batchers[i]
		if b.Timestamp.IsZero() {
			b.Timestamp = now
		}
		b.Checksum = checksum(b)
	}
	root.Collector.Checksum = checksum(&root.Collector)

	out, err := json.MarshalIndent(root, "", "    ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(m.path), 0o755); err != nil {
		return fmt.Errorf("mkdirall: %w", err)
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return os.Rename(tmp, m.path)
}

// checksum computes SHA-256 of v serialised with its Checksum field zeroed.
// Works for both BatcherState and CollectorState via the interface trick below.
func checksum(v any) string {
	type withChecksum interface {
		getChecksum() string
		setChecksum(string)
	}
	switch s := v.(type) {
	case *BatcherState:
		saved := s.Checksum
		s.Checksum = ""
		data, _ := json.Marshal(s)
		s.Checksum = saved
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	case *CollectorState:
		saved := s.Checksum
		s.Checksum = ""
		data, _ := json.Marshal(s)
		s.Checksum = saved
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:])
	}
	_ = v.(withChecksum) // will panic if called with unknown type
	return ""
}

func verifyBatcher(b *BatcherState) error {
	if want := checksum(b); b.Checksum != want {
		return fmt.Errorf("got %s want %s", b.Checksum, want)
	}
	return nil
}

func verifyCollector(c *CollectorState) error {
	if want := checksum(c); c.Checksum != want {
		return fmt.Errorf("got %s want %s", c.Checksum, want)
	}
	return nil
}
