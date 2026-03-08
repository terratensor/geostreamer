package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/terratensor/geostreamer/internal/adapters/readers"
	"github.com/terratensor/geostreamer/internal/core/service"
)

func main() {
	// Парсим флаги командной строки
	csvPath := flag.String("csv", "data.csv", "path to CSV file")
	delimiter := flag.String("delim", "|", "CSV delimiter")
	batchSize := flag.Int("batch", 10000, "batch size for reading")
	checkpointPath := flag.String("checkpoint", "geostreamer.checkpoint", "path to checkpoint file")
	strictMode := flag.Bool("strict", false, "strict mode - stop on any error")
	flag.Parse()

	// Создаем контекст с отменой для graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	// Создаем CSV reader
	delimRune := []rune(*delimiter)[0]
	source, err := readers.NewCSVReader(*csvPath, delimRune, *batchSize, *checkpointPath, *strictMode)
	if err != nil {
		log.Fatalf("Failed to create CSV reader: %v", err)
	}
	defer source.Close()

	// Создаем оркестратор
	orch := service.NewOrchestrator(source)

	// Запускаем обработку
	if err := orch.Process(ctx); err != nil {
		log.Fatalf("Processing failed: %v", err)
	}
}
