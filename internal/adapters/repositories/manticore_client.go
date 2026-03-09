package repositories

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	Manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geostreamer/internal/core/domain"
)

// Config конфигурация клиента Manticore
type Config struct {
	BaseURL    string        // например "http://localhost:9308"
	IndexName  string        // например "geoname_dict"
	Timeout    time.Duration // таймаут запросов
	MaxRetries int           // количество попыток при ошибке
	RetryDelay time.Duration // задержка между попытками
	CacheSize  int           // размер кэша (количество сущностей)
	CacheTTL   time.Duration // время жизни кэша
	BatchSize  int           // максимальный размер батча для запроса
	Workers    int           // количество параллельных воркеров
	DebugMode  bool          // режим отладки
}

// QueryCache простой кэш для результатов запросов
type QueryCache struct {
	mu    sync.RWMutex
	items map[string]cacheItem
	ttl   time.Duration
}

type cacheItem struct {
	hits      []domain.GeoHit
	expiresAt time.Time
}

// NewQueryCache создает новый кэш
func NewQueryCache(ttl time.Duration) *QueryCache {
	return &QueryCache{
		items: make(map[string]cacheItem),
		ttl:   ttl,
	}
}

// Get получает результат из кэша
func (c *QueryCache) Get(key string) ([]domain.GeoHit, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.items[key]
	if !ok {
		return nil, false
	}

	if time.Now().After(item.expiresAt) {
		// Истекло, удалим асинхронно
		go c.Delete(key)
		return nil, false
	}

	return item.hits, true
}

// Set сохраняет результат в кэш
func (c *QueryCache) Set(key string, hits []domain.GeoHit) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items[key] = cacheItem{
		hits:      hits,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Delete удаляет элемент из кэша
func (c *QueryCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

// ManticoreClient реализует интерфейс GeonameRepository
type ManticoreClient struct {
	apiClient   *Manticoresearch.APIClient
	indexName   string
	config      Config
	cache       *QueryCache
	requestPool *sync.Pool
}

// NewManticoreClient создает новый клиент Manticore
func NewManticoreClient(cfg Config) (*ManticoreClient, error) {
	configuration := Manticoresearch.NewConfiguration()

	// Устанавливаем URL сервера
	if len(configuration.Servers) > 0 {
		configuration.Servers[0].URL = cfg.BaseURL
	}

	// ВАЖНО: HTTPClient изначально nil, нужно создать новый
	configuration.HTTPClient = &http.Client{
		Timeout: cfg.Timeout,
	}

	// Проверяем соединение (опционально)
	if cfg.DebugMode {
		resp, err := configuration.HTTPClient.Get(cfg.BaseURL)
		if err != nil {
			log.Printf("Warning: cannot connect to Manticore at %s: %v", cfg.BaseURL, err)
		} else {
			defer resp.Body.Close()
			log.Printf("Connected to Manticore at %s (status: %s)", cfg.BaseURL, resp.Status)
		}
	}

	apiClient := Manticoresearch.NewAPIClient(configuration)

	// Создаем кэш, если указан размер > 0
	var cache *QueryCache
	if cfg.CacheSize > 0 {
		cache = NewQueryCache(cfg.CacheTTL)
	}

	// Пул объектов для построения запросов (для оптимизации памяти)
	requestPool := &sync.Pool{
		New: func() interface{} {
			return make([]string, 0, cfg.BatchSize)
		},
	}

	return &ManticoreClient{
		apiClient:   apiClient,
		indexName:   cfg.IndexName,
		config:      cfg,
		cache:       cache,
		requestPool: requestPool,
	}, nil
}

// FindBatch выполняет поиск нескольких сущностей
func (c *ManticoreClient) FindBatch(ctx context.Context, entityTexts []string) (map[string][]domain.GeoHit, error) {
	result := make(map[string][]domain.GeoHit)

	// Разделяем на кэшированные и некэшированные запросы
	uncached := make([]string, 0)

	for _, text := range entityTexts {
		if c.cache != nil {
			if hits, ok := c.cache.Get(text); ok {
				result[text] = hits
				continue
			}
		}
		uncached = append(uncached, text)
	}

	if len(uncached) == 0 {
		return result, nil
	}

	// Выполняем запросы для некэшированных текстов
	hits, err := c.executeBatchQuery(ctx, uncached)
	if err != nil {
		return nil, fmt.Errorf("batch query failed: %w", err)
	}

	// Группируем результаты по имени
	for _, hit := range hits {
		if _, ok := result[hit.Name]; !ok {
			result[hit.Name] = make([]domain.GeoHit, 0)
		}
		result[hit.Name] = append(result[hit.Name], hit)

		if c.cache != nil {
			c.cache.Set(hit.Name, []domain.GeoHit{hit})
		}
	}

	return result, nil
}

// filterCached фильтрует кэшированные запросы
func (c *ManticoreClient) filterCached(texts []string, result map[string][]domain.GeoHit) []string {
	if c.cache == nil {
		return texts
	}

	uncached := make([]string, 0, len(texts))

	for _, text := range texts {
		if hits, ok := c.cache.Get(text); ok {
			result[text] = hits
		} else {
			uncached = append(uncached, text)
		}
	}

	return uncached
}

// executeBatchQuery выполняет групповой запрос к Manticore
func (c *ManticoreClient) executeBatchQuery(ctx context.Context, texts []string) ([]domain.GeoHit, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Для HTTP API мы не можем использовать OR с большим количеством условий
	// Вместо этого делаем отдельные запросы для каждого текста, но параллельно
	if c.config.DebugMode {
		log.Printf("Processing %d individual queries for batch", len(texts))
	}

	// Создаем канал для результатов
	type queryResult struct {
		text string
		hits []domain.GeoHit
		err  error
	}

	resultChan := make(chan queryResult, len(texts))

	// Ограничиваем параллелизм на уровне запросов
	semaphore := make(chan struct{}, 5) // максимум 5 параллельных запросов

	var wg sync.WaitGroup

	for _, text := range texts {
		wg.Add(1)
		go func(searchText string) {
			defer wg.Done()

			// Захватываем семафор
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Выполняем одиночный запрос
			hits, err := c.executeSingleQuery(ctx, searchText)

			resultChan <- queryResult{
				text: searchText,
				hits: hits,
				err:  err,
			}
		}(text)
	}

	// Закрываем канал после завершения всех горутин
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Собираем результаты
	var allHits []domain.GeoHit
	for result := range resultChan {
		if result.err != nil {
			if c.config.DebugMode {
				log.Printf("Query for '%s' failed: %v", result.text, result.err)
			}
			continue
		}
		allHits = append(allHits, result.hits...)
	}

	return allHits, nil
}

// executeSingleQuery выполняет одиночный запрос к Manticore
func (c *ManticoreClient) executeSingleQuery(ctx context.Context, text string) ([]domain.GeoHit, error) {
	// Экранируем спецсимволы
	escaped := c.escapeString(text)

	// Формируем запрос с точным совпадением
	query := fmt.Sprintf("SELECT * FROM %s WHERE match('\"^%s$\"')",
		c.indexName, escaped)

	if c.config.DebugMode {
		log.Printf("Single query for '%s': %s", text, query)
	}

	// Выполняем запрос с повторными попытками
	var resp *Manticoresearch.SqlResponse
	var err error
	var httpResp *http.Response

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.config.RetryDelay):
			}
		}

		reqCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
		defer cancel()

		resp, httpResp, err = c.apiClient.UtilsAPI.Sql(reqCtx).
			Body(query).
			RawResponse(false).
			Execute()

		if err == nil {
			break
		}

		if c.config.DebugMode && attempt == c.config.MaxRetries {
			log.Printf("Query failed for '%s': %v", text, err)
			if httpResp != nil {
				body, _ := io.ReadAll(httpResp.Body)
				log.Printf("HTTP Status: %s, Body: %s", httpResp.Status, string(body))
				httpResp.Body.Close()
			}
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}

	if err != nil {
		return nil, fmt.Errorf("query failed for '%s': %w", text, err)
	}

	return c.parseResponse(resp)
}

// saveFailedQuery сохраняет проблемный запрос в файл
func (c *ManticoreClient) saveFailedQuery(query string, texts []string, status, body string) {
	filename := fmt.Sprintf("failed_query_%d.log", time.Now().UnixNano())

	content := fmt.Sprintf("Timestamp: %s\nStatus: %s\n\nTexts: %v\n\nQuery: %s\n\nResponse: %s\n",
		time.Now().Format(time.RFC3339),
		status,
		texts,
		query,
		body,
	)

	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		log.Printf("Failed to save failed query: %v", err)
	} else {
		log.Printf("Saved failed query to %s", filename)
	}
}

// min вспомогательная функция
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// escapeString экранирует спецсимволы для Manticore
func (c *ManticoreClient) escapeString(s string) string {
	// Для одиночных запросов достаточно простого экранирования
	var builder strings.Builder
	builder.Grow(len(s) * 2)

	for _, ch := range s {
		switch ch {
		case '\\', '"', '\'', '!', '$', '(', ')', '-', '/', '<', '@', '^', '|', '~':
			builder.WriteByte('\\')
			builder.WriteRune(ch)
		default:
			builder.WriteRune(ch)
		}
	}

	return builder.String()
}

// parseResponse парсит ответ от Manticore в структуры GeoHit
func (c *ManticoreClient) parseResponse(resp *Manticoresearch.SqlResponse) ([]domain.GeoHit, error) {
	if resp == nil {
		return nil, nil
	}

	// Получаем актуальный экземпляр из oneOf обертки
	actualInstance := resp.GetActualInstance()

	switch v := actualInstance.(type) {
	case *Manticoresearch.SqlObjResponse:
		// Это объектный формат ответа (когда rawResponse=false)
		return c.parseSqlObjResponse(v)
	case *[]map[string]interface{}:
		// Это массивный формат ответа (когда rawResponse=true)
		return c.parseRawResponse(v)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", actualInstance)
	}
}

// parseSqlObjResponse парсит ответ в формате SqlObjResponse
func (c *ManticoreClient) parseSqlObjResponse(resp *Manticoresearch.SqlObjResponse) ([]domain.GeoHit, error) {
	if resp == nil {
		return nil, nil
	}

	// Извлекаем hits из ответа
	hits, ok := resp.GetHitsOk()
	if !ok {
		return nil, nil
	}

	// В hits должен быть массив "hits"
	hitsArray, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, nil
	}

	return c.extractHitsFromArray(hitsArray)
}

// parseRawResponse парсит сырой ответ (массив записей)
func (c *ManticoreClient) parseRawResponse(resp *[]map[string]interface{}) ([]domain.GeoHit, error) {
	if resp == nil || len(*resp) == 0 {
		return nil, nil
	}

	result := make([]domain.GeoHit, 0, len(*resp))

	for _, row := range *resp {
		// В сыром ответе каждая строка - это прямая запись
		hit, err := c.rowToGeoHit(row)
		if err != nil {
			continue // пропускаем некорректные строки
		}
		result = append(result, hit)
	}

	return result, nil
}

// extractHitsFromArray извлекает хиты из массива hits
func (c *ManticoreClient) extractHitsFromArray(hitsArray []interface{}) ([]domain.GeoHit, error) {
	result := make([]domain.GeoHit, 0, len(hitsArray))

	for _, hitItem := range hitsArray {
		hitMap, ok := hitItem.(map[string]interface{})
		if !ok {
			continue
		}

		// Извлекаем _source с данными
		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		hit, err := c.sourceToGeoHit(source)
		if err != nil {
			continue
		}
		result = append(result, hit)
	}

	return result, nil
}

// sourceToGeoHit преобразует _source в GeoHit
func (c *ManticoreClient) sourceToGeoHit(source map[string]interface{}) (domain.GeoHit, error) {
	var geoHit domain.GeoHit

	// ID
	if id, ok := source["id"]; ok {
		switch v := id.(type) {
		case float64:
			geoHit.ID = uint64(v)
		case uint64:
			geoHit.ID = v
		case int64:
			geoHit.ID = uint64(v)
		}
	}

	// Name
	if name, ok := source["name"].(string); ok {
		geoHit.Name = name
	}

	// GeohashesString
	if geohashes, ok := source["geohashes_string"].(string); ok {
		geoHit.GeohashesString = geohashes
	}

	// GeohashesUint64 - может быть массивом или строкой
	if geohashesUint, ok := source["geohashes_uint64"]; ok {
		switch v := geohashesUint.(type) {
		case []interface{}:
			geoHit.GeohashesUint64 = make([]uint64, len(v))
			for i, val := range v {
				if f, ok := val.(float64); ok {
					geoHit.GeohashesUint64[i] = uint64(f)
				}
			}
		}
	}

	// Occurrences
	if occ, ok := source["occurrences"]; ok {
		switch v := occ.(type) {
		case float64:
			geoHit.Occurrences = int(v)
		case int:
			geoHit.Occurrences = v
		}
	}

	// FirstGeonameID
	if firstID, ok := source["first_geoname_id"]; ok {
		switch v := firstID.(type) {
		case float64:
			geoHit.FirstGeonameID = uint64(v)
		case uint64:
			geoHit.FirstGeonameID = v
		case int64:
			geoHit.FirstGeonameID = uint64(v)
		}
	}

	return geoHit, nil
}

// rowToGeoHit преобразует сырую строку в GeoHit
func (c *ManticoreClient) rowToGeoHit(row map[string]interface{}) (domain.GeoHit, error) {
	var geoHit domain.GeoHit

	// В сыром ответе поля могут быть прямо в корне
	if id, ok := row["id"]; ok {
		switch v := id.(type) {
		case float64:
			geoHit.ID = uint64(v)
		}
	}

	if name, ok := row["name"].(string); ok {
		geoHit.Name = name
	}

	if geohashes, ok := row["geohashes_string"].(string); ok {
		geoHit.GeohashesString = geohashes
	}

	if geohashesUint, ok := row["geohashes_uint64"]; ok {
		switch v := geohashesUint.(type) {
		case []interface{}:
			geoHit.GeohashesUint64 = make([]uint64, len(v))
			for i, val := range v {
				if f, ok := val.(float64); ok {
					geoHit.GeohashesUint64[i] = uint64(f)
				}
			}
		}
	}

	if occ, ok := row["occurrences"]; ok {
		switch v := occ.(type) {
		case float64:
			geoHit.Occurrences = int(v)
		}
	}

	if firstID, ok := row["first_geoname_id"]; ok {
		switch v := firstID.(type) {
		case float64:
			geoHit.FirstGeonameID = uint64(v)
		}
	}

	return geoHit, nil
}

// FindOne выполняет поиск одной сущности
func (c *ManticoreClient) FindOne(ctx context.Context, entityText string) ([]domain.GeoHit, error) {
	result, err := c.FindBatch(ctx, []string{entityText})
	if err != nil {
		return nil, err
	}
	return result[entityText], nil
}

// Close закрывает соединение
func (c *ManticoreClient) Close() error {
	// HTTP клиент не требует явного закрытия
	return nil
}
