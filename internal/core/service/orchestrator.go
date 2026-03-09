package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/terratensor/geostreamer/internal/adapters/repositories"
	"github.com/terratensor/geostreamer/internal/adapters/writers"
	"github.com/terratensor/geostreamer/internal/core/domain"
	"github.com/terratensor/geostreamer/internal/ports/input"
	"github.com/terratensor/geostreamer/internal/ports/output"
	"github.com/terratensor/geostreamer/internal/ports/repository"
	"github.com/terratensor/geostreamer/pkg/logger"
)

// Orchestrator управляет всем пайплайном обработки
type Orchestrator struct {
	source      input.RecordSource
	repo        repository.GeonameRepository
	writer      output.ResultWriter
	debugWriter *writers.DebugWriter
	workers     int
	batchSize   int // количество строк в батче
	results     map[string]*domain.GeoResult
	mu          sync.RWMutex
	processed   int64 // строки LOC, для которых найдены геохеши
	skipped     int64 // строки LOC, не найденные в Manticore
	filtered    int64 // строки PER/ORG, отфильтрованные по типу
	startTime   time.Time
	flushCount  int // количество doc_id для сброса
	log         *logger.Logger
	statsTicker *time.Ticker
	entityTypes map[string]bool
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

	// Запускаем горутину для вывода статистики
	go o.statsReporter(ctx)

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
	resultErr := o.processResults(ctx, resultsChan)

	// Ждем завершения воркеров
	wg.Wait()
	o.log.Info("All workers completed")

	// Останавливаем таймер статистики
	o.statsTicker.Stop()

	// Проверяем ошибки
	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("error from source: %w", err)
		}
	default:
	}

	// Финальный сброс всех результатов
	if err := o.flushAllResults(ctx); err != nil && !errors.Is(err, context.Canceled) {
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

	var manticoreSuccess, manticoreFailure int64
	if repo, ok := o.repo.(interface{ GetStats() (int64, int64) }); ok {
		manticoreSuccess, manticoreFailure = repo.GetStats()
	}

	elapsed := time.Since(o.startTime)
	rate := float64(o.processed+o.skipped+o.filtered) / elapsed.Seconds()

	o.log.Info("=== STATISTICS ===")
	o.log.Info("Entity types: %v", o.getEntityTypesList())
	o.log.Info("Processed (with geohashes): %d records", o.processed)
	o.log.Info("Skipped (not found in Manticore): %d records", o.skipped)
	o.log.Info("Filtered (other entity types): %d records", o.filtered)
	o.log.Info("Written (unique doc_id): %d records", writerStats.RecordsWritten)
	o.log.Info("Bytes written: %d", writerStats.BytesWritten)
	o.log.Info("Manticore queries: %d success, %d failures", manticoreSuccess, manticoreFailure)
	o.log.Info("Rate: %.2f records/sec", rate)
	o.log.Info("=================")
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
	rate := float64(o.processed+o.skipped+o.filtered) / elapsed.Seconds()

	o.log.Info("=== FINAL STATISTICS ===")
	o.log.Info("Entity types processed: %v", o.getEntityTypesList())
	o.log.Info("Total processing time: %v", elapsed)
	o.log.Info("Records processed (with geohashes): %d", o.processed)
	o.log.Info("Records skipped (not found in Manticore): %d", o.skipped)
	o.log.Info("Records filtered (other types): %d", o.filtered)
	o.log.Info("Records written (unique doc_id): %d", writerStats.RecordsWritten)
	o.log.Info("Bytes written: %d", writerStats.BytesWritten)
	o.log.Info("Manticore queries: %d success, %d failures", manticoreSuccess, manticoreFailure)
	o.log.Info("Processing rate: %.2f records/sec", rate)
	o.log.Info("========================")
}

// getEntityTypesList возвращает список типов для логирования
func (o *Orchestrator) getEntityTypesList() []string {
	types := make([]string, 0, len(o.entityTypes))
	for t := range o.entityTypes {
		types = append(types, t)
	}
	return types
}

// worker обрабатывает записи из канала
func (o *Orchestrator) worker(ctx context.Context, id int, recordsChan <-chan domain.CSVRecord,
	resultsChan chan<- *domain.GeoResult, wg *sync.WaitGroup) {

	defer wg.Done()

	// Храним все записи, сгруппированные по тексту
	batchMap := make(map[string][]domain.CSVRecord)
	batchSize := 0

	for {
		select {
		case <-ctx.Done():
			if batchSize > 0 {
				o.processBatch(ctx, id, batchMap, resultsChan)
			}
			return
		case record, ok := <-recordsChan:
			if !ok {
				if batchSize > 0 {
					o.processBatch(ctx, id, batchMap, resultsChan)
				}
				return
			}

			// Добавляем запись в слайс для данного текста
			batchMap[record.EntityText] = append(batchMap[record.EntityText], record)
			batchSize++

			// Если набрали достаточно записей (по общему количеству строк)
			if batchSize >= o.batchSize {
				o.processBatch(ctx, id, batchMap, resultsChan)
				// Очищаем
				batchMap = make(map[string][]domain.CSVRecord)
				batchSize = 0
			}
		}
	}
}

// processBatch выполняет групповой запрос к Manticore
func (o *Orchestrator) processBatch(ctx context.Context, workerID int,
	batchMap map[string][]domain.CSVRecord, resultsChan chan<- *domain.GeoResult) {

	workerLog := o.log.WithPrefix(fmt.Sprintf("Worker-%d", workerID))

	// Собираем все тексты для запроса (уникальные)
	texts := make([]string, 0, len(batchMap))
	for text := range batchMap {
		texts = append(texts, text)
	}

	// Фильтруем по типу: определяем, какие тексты нужно обрабатывать
	filteredTexts := make([]string, 0)
	filteredMap := make(map[string][]domain.CSVRecord) // только для выбранных типов

	for text, records := range batchMap {
		// Проверяем, есть ли среди записей хотя бы одна с нужным типом
		process := false
		for _, rec := range records {
			if len(o.entityTypes) > 0 && o.entityTypes[rec.EntityType] {
				process = true
				break
			}
		}

		if process {
			filteredTexts = append(filteredTexts, text)
			filteredMap[text] = records
		} else {
			// Все записи этого текста не подходят по типу
			o.mu.Lock()
			o.filtered += int64(len(records))
			o.mu.Unlock()

			// В skipped файл не пишем, так как это не выбранный тип
		}
	}

	if len(filteredTexts) == 0 {
		return
	}

	// Выполняем запрос к Manticore для уникальных текстов
	var hitsMap map[string][]domain.GeoHit
	var infoMap map[string]*repositories.QueryInfo
	var err error

	if repoDebug, ok := o.repo.(interface {
		FindBatchWithInfo(ctx context.Context, entityTexts []string, workerID int) (map[string][]domain.GeoHit, map[string]*repositories.QueryInfo, error)
	}); ok {
		hitsMap, infoMap, err = repoDebug.FindBatchWithInfo(ctx, filteredTexts, workerID)
	} else {
		// Fallback для совместимости
		hitsMap, err = o.repo.FindBatch(ctx, filteredTexts)
	}

	if err != nil {
		workerLog.Error("Batch query failed: %v", err)
		o.mu.Lock()
		// Все записи этого батча идут в skipped
		for _, records := range filteredMap {
			o.skipped += int64(len(records))
		}
		o.mu.Unlock()
		return
	}

	// Обрабатываем результаты для каждого текста
	for text, records := range filteredMap {
		hits := hitsMap[text]
		info := infoMap[text] // может быть nil для fallback

		if len(hits) == 0 {
			// Не найдено в Manticore — все записи этого текста идут в skipped
			o.mu.Lock()
			o.skipped += int64(len(records))
			o.mu.Unlock()

			// Пишем в skipped файл для каждой записи
			for _, rec := range records {
				o.writeSkippedFromInfo(rec, info, "not_found_in_manticore")
			}
			continue
		}

		// Проверяем, есть ли геохеши в найденных хитов
		hasGeohashes := false
		for _, hit := range hits {
			strs, uints := hit.ToGeoResult()
			if len(strs) > 0 || len(uints) > 0 {
				hasGeohashes = true
				break
			}
		}

		if hasGeohashes {
			// Успех — группируем записи по doc_id для агрегации
			tempResults := make(map[string]*domain.GeoResult)

			for _, rec := range records {
				if _, exists := tempResults[rec.DocID]; !exists {
					tempResults[rec.DocID] = domain.NewGeoResult(rec.DocID)
				}
				// Добавляем геохеши из всех хитов (хиты одни для всех записей с этим текстом)
				for _, hit := range hits {
					strs, uints := hit.ToGeoResult()
					for _, s := range strs {
						tempResults[rec.DocID].GeohashesStringMap[s] = struct{}{}
					}
					for _, u := range uints {
						tempResults[rec.DocID].GeohashesUint64Map[u] = struct{}{}
					}
				}
			}

			// Отправляем результаты
			for _, res := range tempResults {
				resultsChan <- res
			}

			o.mu.Lock()
			o.processed += int64(len(records))
			o.mu.Unlock()
		} else {
			// Хиты есть, но без геохешей — все записи в skipped
			o.mu.Lock()
			o.skipped += int64(len(records))
			o.mu.Unlock()

			for _, rec := range records {
				o.writeSkippedFromInfo(rec, info, "hit_without_geohashes")
			}
		}
	}
}

// writeSkippedFromInfo записывает skipped запись в файл (только для выбранных типов)
func (o *Orchestrator) writeSkippedFromInfo(record domain.CSVRecord, info *repositories.QueryInfo, reason string) {
	// Пишем только для выбранных типов (дополнительная проверка)
	if len(o.entityTypes) > 0 && !o.entityTypes[record.EntityType] {
		return
	}

	if o.debugWriter == nil {
		return
	}

	if info == nil {
		// Нет информации о запросе
		skipped := &domain.SkippedRecord{
			Timestamp: time.Now(),
			CSVRecord: record,
			Reason:    reason + "_no_info",
			WorkerID:  -1,
		}
		o.debugWriter.WriteSkipped(skipped)
		return
	}

	queryInfo := &domain.QueryDebugInfo{
		Text:      info.Text,
		Query:     info.Query,
		Attempts:  info.Attempts,
		WorkerID:  info.WorkerID,
		Duration:  info.Duration,
		Timestamp: info.Timestamp,
		HitCount:  len(info.Hits),
		Cached:    info.Cached,
		Response:  info.Response,
	}

	if info.Error != nil {
		queryInfo.Error = info.Error.Error()
	}
	if info.HTTPStatus != "" {
		queryInfo.HTTPStatus = info.HTTPStatus
	}
	if info.HTTPBody != "" {
		queryInfo.HTTPBody = info.HTTPBody
	}

	skipped := &domain.SkippedRecord{
		Timestamp: time.Now(),
		CSVRecord: record,
		Reason:    reason,
		QueryInfo: queryInfo,
		WorkerID:  info.WorkerID,
	}

	o.debugWriter.WriteSkipped(skipped)
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

			// Если набрали достаточно doc_id для сброса
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
	o.statsTicker.Stop()

	if err := o.source.Close(); err != nil {
		return err
	}
	if err := o.repo.Close(); err != nil {
		return err
	}
	return o.writer.Close()
}
