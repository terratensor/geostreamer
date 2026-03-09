package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/terratensor/geostreamer/internal/core/domain"
	"github.com/terratensor/geostreamer/internal/ports/input"
	"github.com/terratensor/geostreamer/internal/ports/output"
	"github.com/terratensor/geostreamer/internal/ports/repository"
)

// Orchestrator управляет всем пайплайном обработки
type Orchestrator struct {
	source     input.RecordSource
	repo       repository.GeonameRepository
	writer     output.ResultWriter
	workers    int
	batchSize  int
	results    map[string]*domain.GeoResult
	mu         sync.RWMutex
	processed  int64
	skipped    int64
	startTime  time.Time
	flushCount int // сбрасывать результаты после N doc_id
}

// NewOrchestrator создает новый оркестратор
func NewOrchestrator(
	source input.RecordSource,
	repo repository.GeonameRepository,
	writer output.ResultWriter,
	workers, batchSize, flushCount int,
) *Orchestrator {
	return &Orchestrator{
		source:     source,
		repo:       repo,
		writer:     writer,
		workers:    workers,
		batchSize:  batchSize,
		flushCount: flushCount,
		results:    make(map[string]*domain.GeoResult),
	}
}

// Process запускает обработку
func (o *Orchestrator) Process(ctx context.Context) error {
	log.Println("Starting processing...")
	o.startTime = time.Now()

	recordsChan, errChan := o.source.ReadRecords(ctx)

	// Создаем дочерний контекст для воркеров
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Создаем пул воркеров
	var wg sync.WaitGroup
	resultsChan := make(chan *domain.GeoResult, o.workers*2)

	// Запускаем воркеры
	for i := 0; i < o.workers; i++ {
		wg.Add(1)
		go o.worker(workerCtx, i, recordsChan, resultsChan, &wg)
	}

	// Закрываем resultsChan после завершения всех воркеров
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Аккумулируем и записываем результаты
	if err := o.processResults(ctx, resultsChan); err != nil {
		return err
	}

	// Проверяем ошибки
	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("error from source: %w", err)
		}
	default:
	}

	// Финальный сброс всех результатов
	if err := o.flushAllResults(ctx); err != nil {
		return fmt.Errorf("failed to flush final results: %w", err)
	}

	// Выводим статистику
	elapsed := time.Since(o.startTime)
	stats := o.writer.GetStats()
	log.Printf("Processing completed. Records: %d, Skipped: %d, Written: %d, Bytes: %d, Time: %s\n",
		o.processed, o.skipped, stats.RecordsWritten, stats.BytesWritten, elapsed)

	return nil
}

// processResults обрабатывает поток результатов
func (o *Orchestrator) processResults(ctx context.Context, resultsChan <-chan *domain.GeoResult) error {
	batch := make([]domain.GeoOutput, 0, o.flushCount)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result, ok := <-resultsChan:
			if !ok {
				// Канал закрыт, записываем остаток
				if len(batch) > 0 {
					if err := o.writer.WriteBatch(ctx, batch); err != nil {
						return err
					}
				}
				return nil
			}

			o.mergeResult(result)

			// Если набрали достаточно для сброса
			if len(o.results) >= o.flushCount {
				batch = o.prepareBatch()
				if err := o.writer.WriteBatch(ctx, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
		}
	}
}

// prepareBatch подготавливает батч для записи
func (o *Orchestrator) prepareBatch() []domain.GeoOutput {
	o.mu.Lock()
	defer o.mu.Unlock()

	batch := make([]domain.GeoOutput, 0, len(o.results))
	for docID, result := range o.results {
		batch = append(batch, result.ToOutput())
		delete(o.results, docID)
	}

	return batch
}

// flushAllResults сбрасывает все оставшиеся результаты
func (o *Orchestrator) flushAllResults(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.results) == 0 {
		return nil
	}

	batch := make([]domain.GeoOutput, 0, len(o.results))
	for _, result := range o.results {
		batch = append(batch, result.ToOutput())
	}

	if err := o.writer.WriteBatch(ctx, batch); err != nil {
		return err
	}

	// Сбрасываем буфер writer на диск
	if err := o.writer.Flush(); err != nil {
		return err
	}

	o.results = make(map[string]*domain.GeoResult)
	return nil
}

// worker обрабатывает записи из канала
func (o *Orchestrator) worker(ctx context.Context, id int, recordsChan <-chan domain.CSVRecord,
	resultsChan chan<- *domain.GeoResult, wg *sync.WaitGroup) {

	defer wg.Done()

	batch := make([]string, 0, o.batchSize)
	batchMap := make(map[string]domain.CSVRecord)

	for {
		select {
		case <-ctx.Done():
			// Получили сигнал завершения, но ДОзавершаем текущий батч
			if len(batch) > 0 {
				o.processBatch(context.Background(), id, batch, batchMap, resultsChan)
			}
			return
		case record, ok := <-recordsChan:
			if !ok {
				if len(batch) > 0 {
					o.processBatch(context.Background(), id, batch, batchMap, resultsChan)
				}
				return
			}

			batch = append(batch, record.EntityText)
			batchMap[record.EntityText] = record

			if len(batch) >= o.batchSize {
				o.processBatch(context.Background(), id, batch, batchMap, resultsChan)
				batch = batch[:0]
				batchMap = make(map[string]domain.CSVRecord)
			}
		}
	}
}

// processBatch выполняет групповой запрос к Manticore
func (o *Orchestrator) processBatch(ctx context.Context, workerID int, texts []string,
	records map[string]domain.CSVRecord, resultsChan chan<- *domain.GeoResult) {

	if o.source.GetProgress()%10000 == 0 {
		log.Printf("Worker %d: processing batch of %d texts (total progress: %d)\n",
			workerID, len(texts), o.source.GetProgress())
	}

	// Выполняем запрос
	hitsMap, err := o.repo.FindBatch(ctx, texts)
	if err != nil {
		log.Printf("Worker %d: batch query failed: %v\n", workerID, err)
		o.skipped += int64(len(texts))
		return
	}

	// Для каждого найденного результата создаем GeoResult
	found := 0
	for text, hits := range hitsMap {
		record, ok := records[text]
		if !ok {
			continue
		}

		// Создаем результат для этого doc_id
		result := domain.NewGeoResult(record.DocID)

		// Добавляем все геохеши из найденных hits
		for _, hit := range hits {
			strings, uints := hit.ToGeoResult()
			for _, s := range strings {
				result.GeohashesStringMap[s] = struct{}{}
			}
			for _, u := range uints {
				result.GeohashesUint64Map[u] = struct{}{}
			}
		}

		// Если есть хотя бы один геохеш, отправляем результат
		if len(result.GeohashesStringMap) > 0 || len(result.GeohashesUint64Map) > 0 {
			resultsChan <- result
			o.processed++
			found++
		} else {
			o.skipped++
		}
	}

	// Записываем пропущенные (не найденные) тексты
	o.skipped += int64(len(texts) - found)
}

// mergeResult объединяет результаты по doc_id
func (o *Orchestrator) mergeResult(result *domain.GeoResult) {
	o.mu.Lock()
	defer o.mu.Unlock()

	existing, ok := o.results[result.DocID]
	if !ok {
		o.results[result.DocID] = result
		return
	}

	// Объединяем множества
	for s := range result.GeohashesStringMap {
		existing.GeohashesStringMap[s] = struct{}{}
	}
	for u := range result.GeohashesUint64Map {
		existing.GeohashesUint64Map[u] = struct{}{}
	}
}

// Close закрывает ресурсы
func (o *Orchestrator) Close() error {
	if err := o.source.Close(); err != nil {
		return err
	}
	if err := o.repo.Close(); err != nil {
		return err
	}
	return o.writer.Close()
}
