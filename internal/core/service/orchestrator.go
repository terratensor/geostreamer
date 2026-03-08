package service

import (
	"context"
	"fmt"
	"log"

	"github.com/terratensor/geostreamer/internal/ports/input"
)

// Orchestrator управляет всем пайплайном обработки
type Orchestrator struct {
	source input.RecordSource
}

// NewOrchestrator создает новый оркестратор
func NewOrchestrator(source input.RecordSource) *Orchestrator {
	return &Orchestrator{
		source: source,
	}
}

// Process запускает обработку
func (o *Orchestrator) Process(ctx context.Context) error {
	log.Println("Starting processing...")

	recordsChan, errChan := o.source.ReadRecords(ctx)

	processed := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case err := <-errChan:
			if err != nil {
				return fmt.Errorf("error from source: %w", err)
			}

		case record, ok := <-recordsChan:
			if !ok {
				// Канал закрыт, обработка завершена
				log.Printf("Processing completed. Total records: %d\n", processed)
				return nil
			}

			// TODO: здесь будет логика обработки
			processed++

			if processed%10000 == 0 {
				log.Printf("Processed %d records...\n", processed)
			}

			// Временно просто логируем первые несколько записей
			if processed <= 5 {
				log.Printf("Record %d: %+v\n", processed, record)
			}
		}
	}
}

// Close закрывает ресурсы
func (o *Orchestrator) Close() error {
	return o.source.Close()
}
