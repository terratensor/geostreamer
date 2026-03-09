package repositories

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	Manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geostreamer/internal/adapters/writers"
	"github.com/terratensor/geostreamer/internal/core/domain"
)

// Config конфигурация клиента Manticore
type Config struct {
	BaseURL         string        // например "http://localhost:9308"
	IndexName       string        // например "geoname_dict"
	Timeout         time.Duration // таймаут запросов
	MaxRetries      int           // количество попыток при ошибке
	RetryDelay      time.Duration // задержка между попытками
	CacheSize       int           // размер кэша (количество сущностей)
	CacheTTL        time.Duration // время жизни кэша
	BatchSize       int           // максимальный размер батча для запроса
	Workers         int           // количество параллельных воркеров
	DebugMode       bool          // режим отладки
	ParallelQueries int           // количество параллельных запросов в батче
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

// QueryInfo содержит полную информацию о запросе
type QueryInfo struct {
	Text       string
	Query      string
	Error      error
	HTTPStatus string
	HTTPBody   string
	Attempts   int
	WorkerID   int
	Duration   time.Duration
	Timestamp  time.Time
	Hits       []domain.GeoHit
	HitCount   int
	Cached     bool
	Response   string
}

// ManticoreClient реализует интерфейс GeonameRepositoryWithDebug
type ManticoreClient struct {
	apiClient    *Manticoresearch.APIClient
	indexName    string
	config       Config
	cache        *QueryCache
	requestPool  *sync.Pool
	successCount int64
	failureCount int64
	failures     []FailedQueryInfo
	mu           sync.RWMutex
	debugWriter  *writers.DebugWriter
}

// FailedQueryInfo содержит детали о неудачном запросе
type FailedQueryInfo struct {
	Text       string
	Query      string
	Error      error
	HTTPStatus string
	HTTPBody   string
	Attempts   int
	WorkerID   int
}

// NewManticoreClient создает новый клиент Manticore
func NewManticoreClient(cfg Config) (*ManticoreClient, error) {
	configuration := Manticoresearch.NewConfiguration()

	if len(configuration.Servers) > 0 {
		configuration.Servers[0].URL = cfg.BaseURL
	}

	configuration.HTTPClient = &http.Client{
		Timeout: cfg.Timeout,
	}

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

	var cache *QueryCache
	if cfg.CacheSize > 0 {
		cache = NewQueryCache(cfg.CacheTTL)
	}

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

// SetDebugWriter implements GeonameRepositoryWithDebug
func (c *ManticoreClient) SetDebugWriter(writer *writers.DebugWriter) {
	c.debugWriter = writer
}

// GetStats implements GeonameRepositoryWithDebug
func (c *ManticoreClient) GetStats() (success, failure int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.successCount, c.failureCount
}

// FindBatch implements GeonameRepository - для обратной совместимости
func (c *ManticoreClient) FindBatch(ctx context.Context, entityTexts []string) (map[string][]domain.GeoHit, error) {
	hits, _, err := c.FindBatchWithInfo(ctx, entityTexts, -1)
	return hits, err
}

// FindBatchWithInfo - ОСНОВНАЯ функция, возвращает и хиты, и информацию
func (c *ManticoreClient) FindBatchWithInfo(ctx context.Context, entityTexts []string, workerID int) (map[string][]domain.GeoHit, map[string]*QueryInfo, error) {
	result := make(map[string][]domain.GeoHit)
	queryInfos := make(map[string]*QueryInfo)

	uncached := make([]string, 0)

	for _, text := range entityTexts {
		if c.cache != nil {
			if hits, ok := c.cache.Get(text); ok {
				result[text] = hits
				queryInfos[text] = &QueryInfo{
					Text:      text,
					Query:     "cached",
					WorkerID:  workerID,
					Timestamp: time.Now(),
					Hits:      hits,
					Cached:    true,
				}
				continue
			}
		}
		uncached = append(uncached, text)
	}

	if len(uncached) == 0 {
		return result, queryInfos, nil
	}

	type queryResult struct {
		text string
		hits []domain.GeoHit
		info *QueryInfo
		err  error
	}

	resultChan := make(chan queryResult, len(uncached))
	batchCtx, batchCancel := context.WithTimeout(context.Background(), c.config.Timeout)
	defer batchCancel()

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, c.config.ParallelQueries)

	for _, text := range uncached {
		wg.Add(1)
		go func(searchText string) {
			defer wg.Done()

			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-batchCtx.Done():
				resultChan <- queryResult{
					text: searchText,
					err:  batchCtx.Err(),
					info: &QueryInfo{
						Text:      searchText,
						Query:     "cancelled",
						WorkerID:  workerID,
						Timestamp: time.Now(),
						Error:     batchCtx.Err(),
					},
				}
				return
			}

			hits, info, err := c.executeSingleQuery(batchCtx, searchText, workerID)

			select {
			case resultChan <- queryResult{
				text: searchText,
				hits: hits,
				info: info,
				err:  err,
			}:
			case <-batchCtx.Done():
			}
		}(text)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	for res := range resultChan {
		if res.err != nil {
			queryInfos[res.text] = res.info
			continue
		}

		result[res.text] = res.hits
		queryInfos[res.text] = res.info

		if c.cache != nil && len(res.hits) > 0 {
			c.cache.Set(res.text, res.hits)
		}
	}

	return result, queryInfos, nil
}

// executeSingleQuery выполняет одиночный запрос
func (c *ManticoreClient) executeSingleQuery(ctx context.Context, text string, workerID int) ([]domain.GeoHit, *QueryInfo, error) {
	escaped := c.escapeString(text)
	query := fmt.Sprintf("SELECT * FROM %s WHERE match('\"^%s$\"')", c.indexName, escaped)

	queryCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()

	info := &QueryInfo{
		Text:      text,
		Query:     query,
		WorkerID:  workerID,
		Timestamp: time.Now(),
		Attempts:  1,
	}

	start := time.Now()
	var resp *Manticoresearch.SqlResponse
	var err error
	var httpResp *http.Response

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			info.Attempts = attempt + 1
			select {
			case <-ctx.Done():
				info.Error = ctx.Err()
				info.Duration = time.Since(start)
				info.HTTPStatus = "context_canceled"
				return nil, info, ctx.Err()
			case <-time.After(c.config.RetryDelay):
			}
		}

		resp, httpResp, err = c.apiClient.UtilsAPI.Sql(queryCtx).
			Body(query).
			RawResponse(false).
			Execute()

		if err == nil {
			c.mu.Lock()
			c.successCount++
			c.mu.Unlock()
			break
		}

		info.Error = err
		if httpResp != nil {
			info.HTTPStatus = httpResp.Status
			body, _ := io.ReadAll(httpResp.Body)
			info.HTTPBody = string(body)
			httpResp.Body.Close()
		}
	}

	info.Duration = time.Since(start)

	if err != nil {
		return nil, info, err
	}

	hits, parseErr := c.parseResponse(resp)
	if parseErr != nil {
		info.Error = parseErr
		return nil, info, parseErr
	}

	if resp != nil {
		if actual := resp.GetActualInstance(); actual != nil {
			if data, marshalErr := json.Marshal(actual); marshalErr == nil {
				info.Response = string(data)
			}
		}
	}

	info.Hits = hits
	info.HitCount = len(hits)
	return hits, info, nil
}

// escapeString экранирует спецсимволы для Manticore
func (c *ManticoreClient) escapeString(s string) string {
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

// parseResponse парсит ответ от Manticore
func (c *ManticoreClient) parseResponse(resp *Manticoresearch.SqlResponse) ([]domain.GeoHit, error) {
	if resp == nil {
		return nil, nil
	}

	actualInstance := resp.GetActualInstance()

	switch v := actualInstance.(type) {
	case *Manticoresearch.SqlObjResponse:
		return c.parseSqlObjResponse(v)
	case *[]map[string]interface{}:
		return c.parseRawResponse(v)
	default:
		return nil, fmt.Errorf("unexpected response type: %T", actualInstance)
	}
}

func (c *ManticoreClient) parseSqlObjResponse(resp *Manticoresearch.SqlObjResponse) ([]domain.GeoHit, error) {
	if resp == nil {
		return nil, nil
	}

	hits, ok := resp.GetHitsOk()
	if !ok {
		return nil, nil
	}

	hitsArray, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, nil
	}

	return c.extractHitsFromArray(hitsArray)
}

func (c *ManticoreClient) parseRawResponse(resp *[]map[string]interface{}) ([]domain.GeoHit, error) {
	if resp == nil || len(*resp) == 0 {
		return nil, nil
	}

	result := make([]domain.GeoHit, 0, len(*resp))

	for _, row := range *resp {
		hit, err := c.rowToGeoHit(row)
		if err != nil {
			continue
		}
		result = append(result, hit)
	}

	return result, nil
}

func (c *ManticoreClient) extractHitsFromArray(hitsArray []interface{}) ([]domain.GeoHit, error) {
	result := make([]domain.GeoHit, 0, len(hitsArray))

	for _, hitItem := range hitsArray {
		hitMap, ok := hitItem.(map[string]interface{})
		if !ok {
			continue
		}

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

func (c *ManticoreClient) sourceToGeoHit(source map[string]interface{}) (domain.GeoHit, error) {
	var geoHit domain.GeoHit

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

	if name, ok := source["name"].(string); ok {
		geoHit.Name = name
	}

	if geohashes, ok := source["geohashes_string"].(string); ok {
		geoHit.GeohashesString = geohashes
	}

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

	if occ, ok := source["occurrences"]; ok {
		switch v := occ.(type) {
		case float64:
			geoHit.Occurrences = int(v)
		case int:
			geoHit.Occurrences = v
		}
	}

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

func (c *ManticoreClient) rowToGeoHit(row map[string]interface{}) (domain.GeoHit, error) {
	var geoHit domain.GeoHit

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
	return nil
}
