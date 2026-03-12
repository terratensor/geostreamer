package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/terratensor/geostreamer/config"
	"github.com/terratensor/geostreamer/internal/adapters/readers"
	"github.com/terratensor/geostreamer/internal/adapters/repositories"
	"github.com/terratensor/geostreamer/internal/adapters/writers"
	"github.com/terratensor/geostreamer/internal/core/service"
	"github.com/terratensor/geostreamer/internal/ports/repository"
	"github.com/terratensor/geostreamer/internal/version"
	"github.com/terratensor/geostreamer/pkg/logger"
)

var (
	// Флаг для вывода версии
	showVersion = flag.Bool("version", false, "show version information and exit")
)

func main() {
	// Сначала парсим флаги, чтобы перехватить -version
	flag.Parse()

	// Если запрошена версия, показываем и выходим
	if *showVersion {
		fmt.Println(version.Info())
		os.Exit(0)
	}

	// Загружаем конфигурацию
	cfg := config.Load()

	// Инициализируем логгер
	if err := logger.Init(cfg.Logging.Level, cfg.Logging.OutputFile); err != nil {
		logger.Get().Error("Failed to initialize logger: %v", err)
		return
	}
	log := logger.Get()

	// Логируем версию при запуске
	log.Info("Starting %s", version.Info())

	// Создаем контекст с отменой
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	// Создаем CSV reader
	delimRune := []rune(cfg.CSV.Delimiter)[0]
	// Создаем CSV reader с новыми параметрами
	source, err := readers.NewCSVReader(
		cfg.CSV.Path,
		delimRune,
		cfg.CSV.BatchSize,    // максимальный размер батча (500-800)
		cfg.CSV.MinBatchSize, // минимальный размер (100)
		cfg.CSV.CheckpointPath,
		cfg.CSV.StrictMode,
		cfg.CSV.DebugMode,
	)
	if err != nil {
		log.Error("Failed to create CSV reader: %v", err)
		return
	}
	defer source.Close()

	// Создаем DebugWriter для ошибок и пропусков
	debugWriter, err := writers.NewDebugWriter(cfg.Output.FailuresPath, cfg.Output.SkippedPath)
	if err != nil {
		log.Error("Failed to create debug writer: %v", err)
		return
	}
	defer debugWriter.Close()

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

	manticoreClient, err := repositories.NewManticoreClient(manticoreCfg)
	if err != nil {
		log.Error("Failed to create Manticore client: %v", err)
		return
	}
	defer manticoreClient.Close()

	// Приводим к интерфейсу для использования в оркестраторе
	var repo repository.GeonameRepository = manticoreClient

	// Если поддерживается расширенный интерфейс, устанавливаем debugWriter
	if repoWithDebug, ok := repo.(repository.GeonameRepositoryWithDebug); ok {
		repoWithDebug.SetDebugWriter(debugWriter)
		log.Info("Debug writer attached to Manticore client")
	}

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
		debugWriter,
		cfg.Manticore.Workers,
		cfg.Manticore.BatchSize,
		10, // flush after 1000 doc_ids
		cfg.Logging.StatsInterval,
		cfg.Filter.EntityTypes,
	)

	// Запускаем обработку
	log.Info("Starting with filters: %v", cfg.Filter.EntityTypes)
	if err := orch.Process(ctx); err != nil && err != context.Canceled {
		log.Error("Processing failed: %v", err)
	}

	// Финальный сброс debugWriter
	debugWriter.Flush()
}
