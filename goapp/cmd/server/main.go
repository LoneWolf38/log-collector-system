package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/LoneWolf38/log-collector-system/goapp/internal/collector"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(env("LOG_LEVEL", "info")),
	}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := collector.Config{
		InputDir:      env("INPUT_DIR", "input"),
		BackupPath:    env("BACKUP_PATH", "backups/restore-template.json"),
		AggregatorURL: env("AGGREGATOR_URL", "http://localhost:9090/ingest"),
		Workers:       envInt("WORKERS", 2),
		BatchSize:     envInt("BATCH_SIZE", 50),
		FlushInterval: time.Duration(envInt("FLUSH_INTERVAL_S", 5)) * time.Second,
	}

	log.Info("collector starting", "input_dir", cfg.InputDir)

	if err := collector.New(log, cfg).Run(ctx); err != nil {
		log.Error("collector error", "err", err)
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func logLevel(s string) slog.Level {
	var l slog.Level
	_ = l.UnmarshalText([]byte(s))
	return l
}
