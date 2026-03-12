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
	source         input.RecordSource
	repo           repository.GeonameRepository
	writer         output.ResultWriter     // режим 1: только геохеши
	nerWriter      *writers.NerWriter      // режим 2: только NER
	enrichedWriter *writers.EnrichedWriter // режим 3: полный
	debugWriter    *writers.DebugWriter
	workers        int
	batchSize      int // количество строк в батче
	results        map[string]*domain.GeoResult
	mu             sync.RWMutex
	processed      int64 // строки LOC, для которых найдены геохеши
	skipped        int64 // строки LOC, не найденные в Manticore
	filtered       int64 // строки PER/ORG, отфильтрованные по типу
	startTime      time.Time
	flushCount     int // количество doc_id для сброса
	log            *logger.Logger
	statsTicker    *time.Ticker
	entityTypes    map[string]bool
}

// NewOrchestrator создает новый оркестратор
func NewOrchestrator(
	source input.RecordSource,
	repo repository.GeonameRepository,
	writer output.ResultWriter, // может быть nil
	nerWriter *writers.NerWriter, // может быть nil
	enrichedWriter *writers.EnrichedWriter, // может быть nil
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
		source:         source,
		repo:           repo,
		writer:         writer,
		nerWriter:      nerWriter,
		enrichedWriter: enrichedWriter,
		debugWriter:    debugWriter,
		workers:        workers,
		batchSize:      batchSize,
		flushCount:     flushCount,
		results:        make(map[string]*domain.GeoResult),
		log:            logger.Get().WithPrefix("Orchestrator"),
		statsTicker:    time.NewTicker(statsInterval),
		entityTypes:    typeMap,
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
	totalRecords := o.processed + o.skipped + o.filtered
	rate := float64(totalRecords) / elapsed.Seconds()

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
	totalRecords := o.processed + o.skipped + o.filtered
	rate := float64(totalRecords) / elapsed.Seconds()

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

// worker обрабатывает батчи из канала
func (o *Orchestrator) worker(ctx context.Context, id int,
	batchChan <-chan []domain.CSVRecord, resultsChan chan<- *domain.GeoResult,
	wg *sync.WaitGroup) {

	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-batchChan:
			if !ok {
				return
			}
			// Обрабатываем батч (все записи одного или нескольких doc_id,
			// но гарантированно без разрывов)
			o.processBatch(ctx, id, batch, resultsChan)
		}
	}
}

// processBatch обрабатывает батч записей
func (o *Orchestrator) processBatch(ctx context.Context, workerID int,
	records []domain.CSVRecord, resultsChan chan<- *domain.GeoResult) {

	// Группируем записи по doc_id (в батче могут быть несколько doc_id,
	// но каждый целый)
	docGroups := make(map[string][]domain.CSVRecord)
	for _, rec := range records {
		docGroups[rec.DocID] = append(docGroups[rec.DocID], rec)
	}

	// Для каждого doc_id собираем уникальные тексты и делаем запрос
	for docID, docRecords := range docGroups {
		o.processDocRecords(ctx, workerID, docID, docRecords, resultsChan)
	}
}

// processDocRecords обрабатывает все записи одного doc_id
func (o *Orchestrator) processDocRecords(ctx context.Context, workerID int,
	docID string, records []domain.CSVRecord, resultsChan chan<- *domain.GeoResult) {

	// 1. Фильтруем записи по выбранным типам из entityTypes
	var filteredRecords []domain.CSVRecord
	for _, rec := range records {
		if len(o.entityTypes) > 0 && o.entityTypes[rec.EntityType] {
			filteredRecords = append(filteredRecords, rec)
		} else {
			// Не выбранный тип - увеличиваем filtered
			o.mu.Lock()
			o.filtered++
			o.mu.Unlock()
		}
	}

	// Если нет записей выбранных типов, ничего не делаем
	if len(filteredRecords) == 0 {
		return
	}

	// 2. Разделяем записи на LOC и не-LOC
	var locRecords []domain.CSVRecord
	var nonLocRecords []domain.CSVRecord // PER и ORG

	for _, rec := range filteredRecords {
		if rec.EntityType == "LOC" {
			locRecords = append(locRecords, rec)
		} else {
			nonLocRecords = append(nonLocRecords, rec)
		}
	}

	// 3. Для LOC записей собираем уникальные тексты и делаем запрос к Manticore
	var hitsMap map[string][]domain.GeoHit
	var infoMap map[string]*repositories.QueryInfo
	var err error

	if len(locRecords) > 0 {
		// Собираем уникальные тексты LOC для запроса
		uniqueLOCtexts := make([]string, 0)
		textMap := make(map[string]bool)
		for _, rec := range locRecords {
			if !textMap[rec.EntityText] {
				textMap[rec.EntityText] = true
				uniqueLOCtexts = append(uniqueLOCtexts, rec.EntityText)
			}
		}

		// Выполняем запрос к Manticore только для LOC
		if repoDebug, ok := o.repo.(interface {
			FindBatchWithInfo(ctx context.Context, entityTexts []string, workerID int) (map[string][]domain.GeoHit, map[string]*repositories.QueryInfo, error)
		}); ok {
			hitsMap, infoMap, err = repoDebug.FindBatchWithInfo(ctx, uniqueLOCtexts, workerID)
		} else {
			// Fallback
			hitsMap, err = o.repo.FindBatch(ctx, uniqueLOCtexts)
		}

		if err != nil {
			// Ошибка запроса - все LOC записи этого doc_id идут в skipped
			o.mu.Lock()
			o.skipped += int64(len(locRecords))
			o.mu.Unlock()

			// Записываем в skipped файл для каждой LOC записи
			for _, rec := range locRecords {
				o.writeSkippedFromInfo(rec, nil, "query_error")
			}
			// Не возвращаемся, продолжаем обработку non-LocRecords
			hitsMap = nil
			infoMap = nil
		}
	}

	// 4. Собираем NER-информацию для ВСЕХ записей
	var enrichedNerLOC []domain.NEREntity
	var enrichedNerPER []domain.NEREntity
	var enrichedNerORG []domain.NEREntity

	// Результат для геохешей (только от LOC с hits)
	result := domain.NewGeoResult(docID)
	hasGeohashes := false

	// Проходим по ВСЕМ записям (locRecords + nonLocRecords)
	allRecords := append(locRecords, nonLocRecords...)

	for _, rec := range allRecords {
		// Для всех типов создаем NER-запись
		nerEntity := domain.NEREntity{
			Value:      rec.EntityText,
			StartPos:   rec.StartPos,
			EndPos:     rec.EndPos,
			Geohash:    []string{}, // по умолчанию пустой массив
			Confidence: rec.Confidence,
		}

		// Для LOC пытаемся найти геохеши
		if rec.EntityType == "LOC" {
			var recordGeohashes []string
			uniqueGeoMap := make(map[string]bool)
			recordHasGeo := false

			if hitsMap != nil {
				hits := hitsMap[rec.EntityText]
				info := infoMap[rec.EntityText]

				if len(hits) > 0 {
					for _, hit := range hits {
						strs, uints := hit.ToGeoResult()
						if len(strs) > 0 || len(uints) > 0 {
							recordHasGeo = true
							hasGeohashes = true

							// Добавляем уникальные строковые геохеши
							for _, s := range strs {
								if !uniqueGeoMap[s] {
									uniqueGeoMap[s] = true
									recordGeohashes = append(recordGeohashes, s)
								}
								// Добавляем в общий результат для doc_id
								result.GeohashesStringMap[s] = struct{}{}
							}

							// Добавляем уникальные uint64 геохеши в основной результат
							for _, u := range uints {
								result.GeohashesUint64Map[u] = struct{}{}
							}
						}
					}
					nerEntity.Geohash = recordGeohashes
				} else {
					// Нет hits - записываем в skipped для LOC
					o.writeSkippedFromInfo(rec, info, "not_found_in_manticore")
				}
			}

			// Обновляем статистику для LOC
			if recordHasGeo {
				o.mu.Lock()
				o.processed++
				o.mu.Unlock()
			} else {
				o.mu.Lock()
				o.skipped++
				o.mu.Unlock()
			}
		}

		// Добавляем в соответствующий NER-массив по типу
		switch rec.EntityType {
		case "LOC":
			enrichedNerLOC = append(enrichedNerLOC, nerEntity)
		case "PER":
			enrichedNerPER = append(enrichedNerPER, nerEntity)
		case "ORG":
			enrichedNerORG = append(enrichedNerORG, nerEntity)
		}
	}

	// Логируем информацию о наличии геохешей в документе (только в debug режиме)
	if hasGeohashes {
		o.log.Debug("Document %s has %d geohashes", docID, len(result.GeohashesStringMap))
	}

	// 5. Формируем GeoOutput для режима 1 (только геохеши)
	geoOutput := result.ToOutput()

	// Режим 1: всегда пишем в обычный writer (если есть и есть геохеши)
	if o.writer != nil && (len(geoOutput.GeohashesString) > 0 || len(geoOutput.GeohashesUint64) > 0) {
		resultsChan <- result
	}

	// 6. Режим 2: NER-only (если включен)
	if o.nerWriter != nil && (len(enrichedNerLOC) > 0 || len(enrichedNerPER) > 0 || len(enrichedNerORG) > 0) {
		nerOutput := &domain.NerOutput{
			DocID:  docID,
			NerLOC: enrichedNerLOC,
			NerPER: enrichedNerPER,
			NerORG: enrichedNerORG,
		}

		// Отправляем NER-only результат (в отдельной горутине чтобы не блокировать)
		go func() {
			batch := []domain.NerOutput{*nerOutput}
			if err := o.nerWriter.WriteBatch(ctx, batch); err != nil {
				o.log.Error("Failed to write NER result for doc_id %s: %v", docID, err)
			}
		}()
	}

	// 7. Режим 3: полный (если включен)
	if o.enrichedWriter != nil {
		enrichedResult := &domain.EnrichedGeoOutput{
			DocID:           docID,
			GeohashesString: geoOutput.GeohashesString,
			GeohashesUint64: geoOutput.GeohashesUint64,
			NerLOC:          enrichedNerLOC,
			NerPER:          enrichedNerPER,
			NerORG:          enrichedNerORG,
		}

		// Отправляем enriched результат (в отдельной горутине чтобы не блокировать)
		go func() {
			batch := []domain.EnrichedGeoOutput{*enrichedResult}
			if err := o.enrichedWriter.WriteBatch(ctx, batch); err != nil {
				o.log.Error("Failed to write enriched result for doc_id %s: %v", docID, err)
			}
		}()
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
