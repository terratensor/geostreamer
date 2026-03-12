package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/terratensor/geostreamer/config"
	"github.com/terratensor/geostreamer/internal/adapters/readers"
	"github.com/terratensor/geostreamer/internal/adapters/repositories"
	"github.com/terratensor/geostreamer/internal/adapters/writers"
	"github.com/terratensor/geostreamer/internal/core/service"
	"github.com/terratensor/geostreamer/internal/ports/output"
	"github.com/terratensor/geostreamer/internal/ports/repository"
	"github.com/terratensor/geostreamer/internal/version"
	"github.com/terratensor/geostreamer/pkg/logger"
)

func main() {
	// Сначала проверяем, не запрошена ли версия (без регистрации флага)
	for _, arg := range os.Args {
		if arg == "-version" || arg == "--version" {
			fmt.Println(version.Info())
			os.Exit(0)
		}
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
	source, err := readers.NewCSVReader(
		cfg.CSV.Path,
		delimRune,
		cfg.CSV.BatchSize,
		cfg.CSV.MinBatchSize,
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

	// Режим 1: Создаем NDJSON writer для обычных результатов (геохеши)
	var writer output.ResultWriter
	if cfg.Output.Path != "" {
		writerCfg := writers.NDJSONWriterConfig{
			FilePath:      cfg.Output.Path,
			FlushInterval: cfg.Output.FlushInterval,
			BufferSize:    cfg.Output.BufferSize,
			UseGzip:       cfg.Output.UseGzip,
		}
		writer, err = writers.NewNDJSONWriter(writerCfg)
		if err != nil {
			log.Error("Failed to create output writer: %v", err)
			return
		}
		defer writer.Close()
		log.Info("Output (geohashes only) enabled: %s", cfg.Output.Path)
	}

	// Режим 2: Создаем NER-only writer если указан путь
	var nerWriter *writers.NerWriter
	if cfg.Output.NerPath != "" {
		nerCfg := writers.NerWriterConfig{
			FilePath:      cfg.Output.NerPath,
			FlushInterval: cfg.Output.FlushInterval,
			BufferSize:    cfg.Output.BufferSize,
		}
		nerWriter, err = writers.NewNerWriter(nerCfg)
		if err != nil {
			log.Error("Failed to create NER writer: %v", err)
			return
		}
		defer nerWriter.Close()
		log.Info("NER-only output enabled: %s", cfg.Output.NerPath)
	}

	// Режим 3: Создаем enriched writer если указан путь
	var enrichedWriter *writers.EnrichedWriter
	if cfg.Output.EnrichedPath != "" {
		enrichedCfg := writers.EnrichedWriterConfig{
			FilePath:      cfg.Output.EnrichedPath,
			FlushInterval: cfg.Output.FlushInterval,
			BufferSize:    cfg.Output.BufferSize,
		}
		enrichedWriter, err = writers.NewEnrichedWriter(enrichedCfg)
		if err != nil {
			log.Error("Failed to create enriched writer: %v", err)
			return
		}
		defer enrichedWriter.Close()
		log.Info("Enriched output enabled: %s", cfg.Output.EnrichedPath)
	}

	// Создаем оркестратор с поддержкой всех трех режимов
	orch := service.NewOrchestrator(
		source,
		repo,
		writer,         // может быть nil
		nerWriter,      // может быть nil
		enrichedWriter, // может быть nil
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

	// Финальный сброс всех writer'ов
	debugWriter.Flush()
	if nerWriter != nil {
		nerWriter.Flush()
	}
	if enrichedWriter != nil {
		enrichedWriter.Flush()
	}
}
