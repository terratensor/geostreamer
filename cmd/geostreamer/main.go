package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/terratensor/geostreamer/config"
	"github.com/terratensor/geostreamer/internal/adapters/readers"
	"github.com/terratensor/geostreamer/internal/adapters/repositories"
	"github.com/terratensor/geostreamer/internal/adapters/writers"
	"github.com/terratensor/geostreamer/internal/core/service"
	"github.com/terratensor/geostreamer/pkg/logger"
)

func main() {
	// Загружаем конфигурацию
	cfg := config.Load()

	// Инициализируем логгер
	if err := logger.Init(cfg.Logging.Level, cfg.Logging.OutputFile); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	log := logger.Get()

	// Создаем контекст с отменой для graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	// Создаем CSV reader
	delimRune := []rune(cfg.CSV.Delimiter)[0]
	source, err := readers.NewCSVReader(
		cfg.CSV.Path,
		delimRune,
		cfg.CSV.BatchSize,
		cfg.CSV.CheckpointPath,
		cfg.CSV.StrictMode,
		cfg.CSV.DebugMode,
	)
	if err != nil {
		log.Error("Failed to create CSV reader: %v", err)
		return
	}
	defer source.Close()

	// Создаем Manticore клиент
	manticoreCfg := repositories.Config{
		BaseURL:         cfg.Manticore.BaseURL,
		IndexName:       cfg.Manticore.IndexName,
		Timeout:         cfg.Manticore.Timeout,
		MaxRetries:      cfg.Manticore.MaxRetries,
		RetryDelay:      cfg.Manticore.RetryDelay,
		CacheSize:       cfg.Manticore.CacheSize,
		CacheTTL:        cfg.Manticore.CacheTTL,
		BatchSize:       cfg.Manticore.BatchSize,
		Workers:         cfg.Manticore.Workers,
		DebugMode:       cfg.Manticore.DebugMode,
		ParallelQueries: cfg.Manticore.ParallelQueries,
	}

	repo, err := repositories.NewManticoreClient(manticoreCfg)
	if err != nil {
		log.Error("Failed to create Manticore client: %v", err)
		return
	}
	defer repo.Close()

	// Создаем NDJSON writer
	writerCfg := writers.NDJSONWriterConfig{
		FilePath:      cfg.Output.Path,
		FlushInterval: cfg.Output.FlushInterval,
		BufferSize:    cfg.Output.BufferSize,
		UseGzip:       cfg.Output.UseGzip,
	}

	writer, err := writers.NewNDJSONWriter(writerCfg)
	if err != nil {
		log.Error("Failed to create output writer: %v", err)
		return
	}
	defer writer.Close()

	// Создаем оркестратор
	orch := service.NewOrchestrator(
		source,
		repo,
		writer,
		cfg.Manticore.Workers,
		cfg.Manticore.BatchSize,
		10, // flush after 10 doc_ids
		cfg.Logging.StatsInterval,
	)

	// Запускаем обработку
	if err := orch.Process(ctx); err != nil && err != context.Canceled {
		log.Error("Processing failed: %v", err)
	}
}
