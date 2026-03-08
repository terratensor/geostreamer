package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/terratensor/geostreamer/internal/core/domain"
	"github.com/terratensor/geostreamer/internal/ports/input"
	"github.com/terratensor/geostreamer/internal/ports/repository"
)

// Orchestrator управляет всем пайплайном обработки
type Orchestrator struct {
	source    input.RecordSource
	repo      repository.GeonameRepository
	workers   int
	batchSize int
	results   map[string]*domain.GeoResult
	mu        sync.RWMutex
	processed int64
	skipped   int64
	startTime time.Time
}

// NewOrchestrator создает новый оркестратор
func NewOrchestrator(source input.RecordSource, repo repository.GeonameRepository, workers, batchSize int) *Orchestrator {
	return &Orchestrator{
		source:    source,
		repo:      repo,
		workers:   workers,
		batchSize: batchSize,
		results:   make(map[string]*domain.GeoResult),
	}
}

// Process запускает обработку
func (o *Orchestrator) Process(ctx context.Context) error {
	log.Println("Starting processing...")
	o.startTime = time.Now()

	recordsChan, errChan := o.source.ReadRecords(ctx)

	// Создаем пул воркеров
	var wg sync.WaitGroup
	resultsChan := make(chan *domain.GeoResult, o.workers*2)

	// Запускаем воркеры
	for i := 0; i < o.workers; i++ {
		wg.Add(1)
		go o.worker(ctx, i, recordsChan, resultsChan, &wg)
	}

	// Закрываем resultsChan после завершения всех воркеров
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Аккумулируем результаты
	for result := range resultsChan {
		o.mergeResult(result)
	}

	// Проверяем ошибки
	select {
	case err := <-errChan:
		if err != nil {
			return fmt.Errorf("error from source: %w", err)
		}
	default:
	}

	// Выводим статистику
	elapsed := time.Since(o.startTime)
	log.Printf("Processing completed. Records: %d, Skipped: %d, Time: %s\n",
		o.processed, o.skipped, elapsed)

	return nil
}

// worker обрабатывает записи из канала
func (o *Orchestrator) worker(ctx context.Context, id int, recordsChan <-chan domain.CSVRecord,
	resultsChan chan<- *domain.GeoResult, wg *sync.WaitGroup) {

	defer wg.Done()

	// Собираем батч для запроса
	batch := make([]string, 0, o.batchSize)
	batchMap := make(map[string]domain.CSVRecord)

	for record := range recordsChan {
		// Добавляем в текущий батч
		batch = append(batch, record.EntityText)
		batchMap[record.EntityText] = record

		// Если батч набран, выполняем запрос
		if len(batch) >= o.batchSize {
			o.processBatch(ctx, id, batch, batchMap, resultsChan)

			// Очищаем батч
			batch = batch[:0]
			batchMap = make(map[string]domain.CSVRecord)
		}
	}

	// Обрабатываем остаток
	if len(batch) > 0 {
		o.processBatch(ctx, id, batch, batchMap, resultsChan)
	}
}

// processBatch выполняет групповой запрос к Manticore
func (o *Orchestrator) processBatch(ctx context.Context, workerID int, texts []string,
	records map[string]domain.CSVRecord, resultsChan chan<- *domain.GeoResult) {

	log.Printf("Worker %d: processing batch of %d texts\n", workerID, len(texts))

	// Выполняем запрос
	hitsMap, err := o.repo.FindBatch(ctx, texts)
	if err != nil {
		log.Printf("Worker %d: batch query failed: %v\n", workerID, err)
		o.skipped += int64(len(texts))
		return
	}

	// Для каждого найденного результата создаем GeoResult
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
		} else {
			o.skipped++
		}
	}

	// Записываем пропущенные (не найденные) тексты
	o.skipped += int64(len(texts) - len(hitsMap))
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
	return o.repo.Close()
}
