package repositories

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	Manticoresearch "github.com/manticoresoftware/manticoresearch-go"
	"github.com/terratensor/geostreamer/internal/core/domain"
)

// ManticoreClient реализует интерфейс GeonameRepository
type ManticoreClient struct {
	apiClient   *Manticoresearch.APIClient
	indexName   string
	config      Config
	cache       *QueryCache
	requestPool *sync.Pool
}

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

// NewManticoreClient создает новый клиент Manticore
func NewManticoreClient(cfg Config) (*ManticoreClient, error) {
	configuration := Manticoresearch.NewConfiguration()
	configuration.Servers[0].URL = cfg.BaseURL
	configuration.HTTPClient.Timeout = cfg.Timeout

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

// FindBatch выполняет поиск нескольких сущностей за один запрос
func (c *ManticoreClient) FindBatch(ctx context.Context, entityTexts []string) (map[string][]domain.GeoHit, error) {
	result := make(map[string][]domain.GeoHit)

	// Разделяем на кэшированные и некэшированные запросы
	uncached := c.filterCached(entityTexts, result)

	if len(uncached) == 0 {
		return result, nil
	}

	// Выполняем запрос к Manticore для некэшированных
	hits, err := c.executeBatchQuery(ctx, uncached)
	if err != nil {
		return nil, fmt.Errorf("batch query failed: %w", err)
	}

	// Группируем результаты по entity_text
	for _, hit := range hits {
		// В SQL ответе нет прямого указания на исходный запрос,
		// поэтому используем name как ключ
		if _, ok := result[hit.Name]; !ok {
			result[hit.Name] = make([]domain.GeoHit, 0)
		}
		result[hit.Name] = append(result[hit.Name], hit)

		// Сохраняем в кэш
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

	// Строим SQL запрос с OR для всех текстов
	conditions := make([]string, len(texts))
	for i, text := range texts {
		// Экранируем кавычки и спецсимволы
		escaped := c.escapeString(text)
		conditions[i] = fmt.Sprintf("match('\"^%s$\"')", escaped)
	}

	query := fmt.Sprintf("SELECT * FROM %s WHERE %s",
		c.indexName,
		strings.Join(conditions, " OR "))

	// Выполняем запрос с повторными попытками
	var resp *Manticoresearch.SqlResponse
	var err error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.config.RetryDelay):
			}
		}

		resp, _, err = c.apiClient.UtilsAPI.Sql(ctx).
			Body(query).
			RawResponse(false). // Получаем структурированный ответ
			Execute()

		if err == nil {
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("query failed after %d attempts: %w", c.config.MaxRetries+1, err)
	}

	// Парсим ответ
	return c.parseResponse(resp)
}

// escapeString экранирует спецсимволы для Manticore
func (c *ManticoreClient) escapeString(s string) string {
	// Экранируем обратную косую черту и кавычки
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// parseResponse парсит ответ от Manticore в структуры GeoHit
func (c *ManticoreClient) parseResponse(resp *Manticoresearch.SqlResponse) ([]domain.GeoHit, error) {
	if resp == nil {
		return nil, nil
	}

	// В SqlResponse.Hits может быть массив объектов
	hits, ok := resp.Hits.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected response format: hits is not an object")
	}

	hitsArray, ok := hits["hits"].([]interface{})
	if !ok {
		// Может быть пустой ответ
		return nil, nil
	}

	result := make([]domain.GeoHit, 0, len(hitsArray))

	for _, hit := range hitsArray {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		// Извлекаем _source с данными
		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

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

		result = append(result, geoHit)
	}

	return result, nil
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
