package config

import (
	"flag"
	"time"
)

// Config общая конфигурация приложения
type Config struct {
	CSV struct {
		Path           string
		Delimiter      string
		BatchSize      int
		CheckpointPath string
		StrictMode     bool
		DebugMode      bool
	}

	Manticore struct {
		BaseURL    string
		IndexName  string
		Timeout    time.Duration
		MaxRetries int
		RetryDelay time.Duration
		CacheSize  int
		CacheTTL   time.Duration
		BatchSize  int
		Workers    int
		DebugMode  bool
	}

	Output struct {
		Path          string
		FlushInterval time.Duration
		BufferSize    int
		UseGzip       bool
		PrettyPrint   bool
	}
}

// Load загружает конфигурацию из флагов командной строки
func Load() *Config {
	var cfg Config

	// CSV flags
	flag.StringVar(&cfg.CSV.Path, "csv", "data.csv", "path to CSV file")
	flag.StringVar(&cfg.CSV.Delimiter, "delim", "|", "CSV delimiter")
	flag.IntVar(&cfg.CSV.BatchSize, "csv-batch", 10000, "CSV read batch size")
	flag.StringVar(&cfg.CSV.CheckpointPath, "checkpoint", "geostreamer.checkpoint", "path to checkpoint file")
	flag.BoolVar(&cfg.CSV.StrictMode, "strict", false, "strict mode - stop on any error")
	flag.BoolVar(&cfg.CSV.DebugMode, "debug", false, "debug mode - show detailed info")

	// Manticore flags
	flag.StringVar(&cfg.Manticore.BaseURL, "manticore-url", "http://localhost:9308", "Manticore Search URL")
	flag.StringVar(&cfg.Manticore.IndexName, "manticore-index", "geoname_dict", "Manticore index name")
	flag.DurationVar(&cfg.Manticore.Timeout, "manticore-timeout", 60*time.Second, "Manticore query timeout")
	flag.IntVar(&cfg.Manticore.MaxRetries, "manticore-retries", 3, "Manticore query retries")
	flag.DurationVar(&cfg.Manticore.RetryDelay, "manticore-retry-delay", 2*time.Second, "Delay between retries")
	flag.IntVar(&cfg.Manticore.CacheSize, "manticore-cache-size", 10000, "Manticore query cache size")
	flag.DurationVar(&cfg.Manticore.CacheTTL, "manticore-cache-ttl", 1*time.Hour, "Manticore cache TTL")
	flag.IntVar(&cfg.Manticore.BatchSize, "manticore-batch", 100, "Manticore batch query size")
	flag.IntVar(&cfg.Manticore.Workers, "manticore-workers", 10, "Number of parallel Manticore workers")
	flag.BoolVar(&cfg.Manticore.DebugMode, "manticore-debug", false, "Manticore client debug mode")

	// Output flags
	flag.StringVar(&cfg.Output.Path, "output", "results.ndjson", "Path to output NDJSON file")
	flag.DurationVar(&cfg.Output.FlushInterval, "output-flush", 5*time.Second, "Output file flush interval")
	flag.IntVar(&cfg.Output.BufferSize, "output-buffer", 1024*1024, "Output buffer size in bytes")
	flag.BoolVar(&cfg.Output.UseGzip, "output-gzip", false, "Compress output file with gzip")
	flag.BoolVar(&cfg.Output.PrettyPrint, "output-pretty", false, "Pretty print JSON (slower, larger files)")

	flag.Parse()

	return &cfg
}
