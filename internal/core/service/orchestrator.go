package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/terratensor/geostreamer/internal/adapters/writers"
	"github.com/terratensor/geostreamer/internal/core/domain"
	"github.com/terratensor/geostreamer/internal/ports/input"
	"github.com/terratensor/geostreamer/internal/ports/output"
	"github.com/terratensor/geostreamer/internal/ports/repository"
	"github.com/terratensor/geostreamer/pkg/logger"
)

// Orchestrator с поддержкой фильтрации и отладки
type Orchestrator struct {
	source      input.RecordSource
	repo        repository.GeonameRepository
	writer      output.ResultWriter
	debugWriter *writers.DebugWriter
	workers     int
	batchSize   int
	results     map[string]*domain.GeoResult
	mu          sync.RWMutex
	processed   int64
	skipped     int64
	startTime   time.Time
	flushCount  int
	log         *logger.Logger
	statsTicker *time.Ticker
	entityTypes map[string]bool // для быстрой проверки
}

// NewOrchestrator создает новый оркестратор
func NewOrchestrator(
	source input.RecordSource,
	repo repository.GeonameRepository,
	writer output.ResultWriter,
	debugWriter *writers.DebugWriter,
	workers, batchSize, flushCount int,
	statsInterval time.Duration,
	entityTypes []string,
) *Orchestrator {
	// Создаем map для быстрой проверки
	typeMap := make(map[string]bool)
	for _, t := range entityTypes {
		typeMap[t] = true
	}

	return &Orchestrator{
		source:      source,
		repo:        repo,
		writer:      writer,
		debugWriter: debugWriter,
		workers:     workers,
		batchSize:   batchSize,
		flushCount:  flushCount,
		results:     make(map[string]*domain.GeoResult),
		log:         logger.Get().WithPrefix("Orchestrator"),
		statsTicker: time.NewTicker(statsInterval),
		entityTypes: typeMap,
	}
}

// Process запускает обработку
func (o *Orchestrator) Process(ctx context.Context) error {
	o.log.Info("Starting processing...")
	o.startTime = time.Now()

	recordsChan, errChan := o.source.ReadRecords(ctx)

	// Запускаем горутину для вывода статистики
	go o.statsReporter(ctx)

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

	// Канал для сигнала о завершении всех воркеров
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Аккумулируем и записываем результаты
	resultErr := o.processResults(ctx, resultsChan)

	// Ждем завершения воркеров или сигнала отмены
	select {
	case <-done:
		o.log.Info("All workers completed")
	case <-ctx.Done():
		o.log.Warn("Context canceled, waiting for workers to finish...")
		select {
		case <-done:
			o.log.Info("Workers finished gracefully")
		case <-time.After(5 * time.Second):
			o.log.Warn("Workers did not finish in time, forcing shutdown")
		}
	}

	// Проверяем ошибки
	select {
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			return fmt.Errorf("error from source: %w", err)
		}
	default:
	}

	// Финальный сброс всех результатов
	if err := o.flushAllResults(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("failed to flush final results: %w", err)
	}

	// Выводим финальную статистику
	o.printFinalStats()

	return resultErr
}

// statsReporter периодически выводит статистику
func (o *Orchestrator) statsReporter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			o.statsTicker.Stop()
			return
		case <-o.statsTicker.C:
			o.printStats()
		}
	}
}

// printStats выводит текущую статистику
func (o *Orchestrator) printStats() {
	o.mu.RLock()
	defer o.mu.RUnlock()

	writerStats := o.writer.GetStats()

	// Если у клиента есть метод GetStats, вызываем его
	var manticoreSuccess, manticoreFailure int64
	if repo, ok := o.repo.(interface{ GetStats() (int64, int64) }); ok {
		manticoreSuccess, manticoreFailure = repo.GetStats()
	}

	stats := logger.Stats{
		ProcessedRecords: o.processed,
		SkippedRecords:   o.skipped,
		WrittenRecords:   writerStats.RecordsWritten,
		BytesWritten:     writerStats.BytesWritten,
		ManticoreSuccess: manticoreSuccess,
		ManticoreFailure: manticoreFailure,
		ProcessingTime:   time.Since(o.startTime),
	}

	o.log.PrintStats(stats)
}

// printFinalStats выводит финальную статистику
func (o *Orchestrator) printFinalStats() {
	o.mu.RLock()
	defer o.mu.RUnlock()

	writerStats := o.writer.GetStats()

	var manticoreSuccess, manticoreFailure int64
	if repo, ok := o.repo.(interface{ GetStats() (int64, int64) }); ok {
		manticoreSuccess, manticoreFailure = repo.GetStats()
	}

	elapsed := time.Since(o.startTime)

	o.log.Info("=== FINAL STATISTICS ===")
	o.log.Info("Total processing time: %v", elapsed)
	o.log.Info("Records processed: %d", o.processed)
	o.log.Info("Records skipped: %d", o.skipped)
	o.log.Info("Records written: %d", writerStats.RecordsWritten)
	o.log.Info("Bytes written: %d", writerStats.BytesWritten)
	o.log.Info("Manticore queries: %d success, %d failures",
		manticoreSuccess, manticoreFailure)

	if o.processed > 0 {
		rate := float64(o.processed) / elapsed.Seconds()
		o.log.Info("Processing rate: %.2f records/sec", rate)
	}
	o.log.Info("========================")
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
				if len(batch) > 0 {
					if err := o.writer.WriteBatch(ctx, batch); err != nil {
						return err
					}
				}
				return nil
			}

			o.mergeResult(result)

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

	workerLog := o.log.WithPrefix(fmt.Sprintf("Worker-%d", id))

	batch := make([]string, 0, o.batchSize)
	batchMap := make(map[string]domain.CSVRecord)

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				workerLog.Debug("Finishing with %d remaining texts", len(batch))
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
				o.processBatch(ctx, id, batch, batchMap, resultsChan)
				batch = batch[:0]
				batchMap = make(map[string]domain.CSVRecord)
			}
		}
	}
}

// processBatch с фильтрацией и сбором ошибок
func (o *Orchestrator) processBatch(ctx context.Context, workerID int, texts []string,
	records map[string]domain.CSVRecord, resultsChan chan<- *domain.GeoResult) {

	workerLog := o.log.WithPrefix(fmt.Sprintf("Worker-%d", workerID))

	// Фильтруем только нужные типы сущностей
	filteredTexts := make([]string, 0)
	filteredRecords := make(map[string]domain.CSVRecord)

	for text, record := range records {
		// Проверяем тип сущности
		if len(o.entityTypes) > 0 {
			if !o.entityTypes[record.EntityType] {
				// Пропускаем ненужный тип
				o.skipped++
				if o.debugWriter != nil {
					skipped := &domain.SkippedRecord{
						Timestamp: time.Now(),
						CSVRecord: record,
						Reason: fmt.Sprintf("filtered: entity_type=%s not in %v",
							record.EntityType, o.entityTypes),
						WorkerID: workerID,
					}
					o.debugWriter.WriteSkipped(skipped)
				}
				continue
			}
		}
		filteredTexts = append(filteredTexts, text)
		filteredRecords[text] = record
	}

	if len(filteredTexts) == 0 {
		return
	}

	if o.source.GetProgress()%10000 == 0 {
		workerLog.Info("Processing batch of %d texts (filtered from %d), total progress: %d",
			len(filteredTexts), len(texts), o.source.GetProgress())
	}

	// Передаем workerID в запрос для отслеживания
	if repo, ok := o.repo.(interface {
		FindBatchWithWorker(ctx context.Context, texts []string, workerID int) (map[string][]domain.GeoHit, error)
	}); ok {
		hitsMap, err := repo.FindBatchWithWorker(ctx, filteredTexts, workerID)
		if err != nil {
			workerLog.Error("Batch query failed: %v", err)
			o.skipped += int64(len(filteredTexts))
			return
		}

		o.processHits(hitsMap, filteredRecords, resultsChan, workerID)
	} else {
		// Fallback для совместимости
		hitsMap, err := o.repo.FindBatch(ctx, filteredTexts)
		if err != nil {
			workerLog.Error("Batch query failed: %v", err)
			o.skipped += int64(len(filteredTexts))
			return
		}
		o.processHits(hitsMap, filteredRecords, resultsChan, workerID)
	}
}

// processHits обрабатывает результаты запросов
func (o *Orchestrator) processHits(
	hitsMap map[string][]domain.GeoHit,
	records map[string]domain.CSVRecord,
	resultsChan chan<- *domain.GeoResult,
	workerID int,
) {
	found := 0
	for text, hits := range hitsMap {
		record, ok := records[text]
		if !ok {
			continue
		}

		result := domain.NewGeoResult(record.DocID)

		for _, hit := range hits {
			strings, uints := hit.ToGeoResult()
			for _, s := range strings {
				result.GeohashesStringMap[s] = struct{}{}
			}
			for _, u := range uints {
				result.GeohashesUint64Map[u] = struct{}{}
			}
		}

		if len(result.GeohashesStringMap) > 0 || len(result.GeohashesUint64Map) > 0 {
			resultsChan <- result
			o.processed++
			found++
		} else {
			o.skipped++
			if o.debugWriter != nil {
				skipped := &domain.SkippedRecord{
					Timestamp: time.Now(),
					CSVRecord: record,
					Reason:    "not_found_in_manticore",
					WorkerID:  workerID,
				}
				o.debugWriter.WriteSkipped(skipped)
			}
		}
	}

	// Пропущенные (не найденные) тексты
	notFound := len(records) - found
	if notFound > 0 {
		o.skipped += int64(notFound)
	}
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

	for s := range result.GeohashesStringMap {
		existing.GeohashesStringMap[s] = struct{}{}
	}
	for u := range result.GeohashesUint64Map {
		existing.GeohashesUint64Map[u] = struct{}{}
	}
}

// Close закрывает ресурсы
func (o *Orchestrator) Close() error {
	o.statsTicker.Stop()

	if err := o.source.Close(); err != nil {
		return err
	}
	if err := o.repo.Close(); err != nil {
		return err
	}
	return o.writer.Close()
}
