package main

import (
	"bytes"
	"compress/zlib"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"
)

type QueryProfile struct {
	QueryID     string            `json:"query_id"`
	User        string            `json:"user"`
	Database    string            `json:"database"`
	State       string            `json:"state"`
	StartTime   string            `json:"start_time"`
	EndTime     string            `json:"end_time"`
	Summary     Summary           `json:"summary"`
	Fragments   []Fragment        `json:"fragments"`
	Timeline    []TimelineEvent   `json:"timeline"`
	Resources   ResourceUsage     `json:"resources"`
	QueryConfig map[string]string `json:"query_config"`
}

type Summary struct {
	TotalTimeMs     int `json:"total_time_ms"`
	PlanningTimeMs  int `json:"planning_time_ms"`
	ExecutionTimeMs int `json:"execution_time_ms"`
	RowsProduced    int `json:"rows_produced"`
	PeakMemoryMB    int `json:"peak_memory_mb"`
}

type Fragment struct {
	ID            int        `json:"id"`
	Hosts         []string   `json:"hosts"`
	InstanceCount int        `json:"instance_count"`
	AvgTimeMs     int        `json:"avg_time_ms"`
	MaxTimeMs     int        `json:"max_time_ms"`
	Operators     []Operator `json:"operators"`
}

type Operator struct {
	ID       int        `json:"id"`
	Type     string     `json:"type"`
	Metrics  Metrics    `json:"metrics"`
	Children []Operator `json:"children,omitempty"`
}

type Metrics struct {
	RowsRead     int `json:"rows_read,omitempty"`
	RowsReturned int `json:"rows_returned,omitempty"`
	BytesReadMB  int `json:"bytes_read_mb,omitempty"`
	ExecTimeMs   int `json:"exec_time_ms"`
	MemoryMB     int `json:"memory_mb,omitempty"`
}

type TimelineEvent struct {
	Event  string `json:"event"`
	TimeMs int    `json:"time_ms"`
}

type ResourceUsage struct {
	PerNodeMemoryMB int `json:"per_node_memory_mb"`
	TotalMemoryMB   int `json:"total_memory_mb"`
	Threads         int `json:"threads"`
	DiskSpillMB     int `json:"disk_spill_mb"`
}

func generateDummyProfile() QueryProfile {
	now := time.Now()
	qid, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		panic(err)
	}
	rp, err := rand.Int(rand.Reader, big.NewInt(10000000))
	if err != nil {
		panic(err)
	}
	return QueryProfile{
		QueryID:   fmt.Sprintf("query-%d", qid),
		User:      "test_user",
		Database:  "default",
		State:     "FINISHED",
		StartTime: now.Format(time.RFC3339),
		EndTime:   now.Add(2 * time.Second).Format(time.RFC3339),

		Summary: Summary{
			TotalTimeMs:     2000,
			PlanningTimeMs:  200,
			ExecutionTimeMs: 1800,
			RowsProduced:    int(rp.Int64()),
			PeakMemoryMB:    512,
		},

		Fragments: []Fragment{
			{
				ID:            0,
				Hosts:         []string{"host1", "host2"},
				InstanceCount: 2,
				AvgTimeMs:     900,
				MaxTimeMs:     1200,
				Operators: []Operator{
					{
						ID:   1,
						Type: "HASH_JOIN",
						Metrics: Metrics{
							ExecTimeMs: 1200,
							MemoryMB:   256,
						},
						Children: []Operator{
							{
								ID:   2,
								Type: "SCAN",
								Metrics: Metrics{
									RowsRead:    500000,
									BytesReadMB: 300,
									ExecTimeMs:  800,
								},
							},
							{
								ID:   3,
								Type: "SCAN",
								Metrics: Metrics{
									RowsRead:    200000,
									BytesReadMB: 120,
									ExecTimeMs:  400,
								},
							},
						},
					},
				},
			},
		},

		Timeline: []TimelineEvent{
			{"Submitted", 0},
			{"Planning Finished", 200},
			{"Execution Started", 250},
			{"Completed", 2000},
		},

		Resources: ResourceUsage{
			PerNodeMemoryMB: 256,
			TotalMemoryMB:   512,
			Threads:         8,
			DiskSpillMB:     64,
		},

		QueryConfig: map[string]string{
			"MEM_LIMIT": "4g",
			"MT_DOP":    "2",
		},
	}
}

func newUUID() (string, error) {
	u := make([]byte, 16)

	_, err := rand.Read(u)
	if err != nil {
		return "", err
	}

	// Set version (4) and variant (RFC 4122)
	u[6] = (u[6] & 0x0f) | 0x40 // Version 4
	u[8] = (u[8] & 0x3f) | 0x80 // Variant is 10

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		u[0:4],
		u[4:6],
		u[6:8],
		u[8:10],
		u[10:16],
	), nil
}

func compressZlib(data []byte) ([]byte, error) {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	w.Close()
	return b.Bytes(), nil
}

func main() {
	err := os.MkdirAll("input", 0755)
	if err != nil {
		panic(err)
	}
	for range 10 {
		log_file_name := fmt.Sprintf("input/ts_log_%d", time.Now().Unix())
		logs := []string{}
		for range 100 {
			profile := generateDummyProfile()

			jsonData, err := json.MarshalIndent(profile, "", "  ")
			if err != nil {
				panic(err)
			}

			compressed, err := compressZlib(jsonData)
			if err != nil {
				panic(err)
			}

			base64E := base64.StdEncoding.EncodeToString(compressed)

			uuid, err := newUUID()
			if err != nil {
				panic(err)
			}
			d := fmt.Sprintf("%d\t%s\t%s", time.Now().Unix(), uuid, base64E)

			logs = append(logs, d)
		}
		time.Sleep(4 * time.Second)
		err := os.WriteFile(log_file_name, []byte(strings.Join(logs, "\n")), 0644)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Generated %s\n", log_file_name)
	}
}
