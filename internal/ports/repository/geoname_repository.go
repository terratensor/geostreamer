package repository

import (
	"context"

	"github.com/terratensor/geostreamer/internal/adapters/repositories"
	"github.com/terratensor/geostreamer/internal/adapters/writers"
	"github.com/terratensor/geostreamer/internal/core/domain"
)

// GeonameRepository определяет интерфейс для поиска географических объектов
type GeonameRepository interface {
	// FindBatch выполняет поиск нескольких сущностей за один запрос
	FindBatch(ctx context.Context, entityTexts []string) (map[string][]domain.GeoHit, error)

	// FindOne выполняет поиск одной сущности
	FindOne(ctx context.Context, entityText string) ([]domain.GeoHit, error)

	// Close закрывает соединение с репозиторием
	Close() error
}

// GeonameRepositoryWithDebug расширенный интерфейс для отладки
type GeonameRepositoryWithDebug interface {
	GeonameRepository
	// FindBatchWithInfo выполняет поиск с возвратом детальной информации
	FindBatchWithInfo(ctx context.Context, entityTexts []string, workerID int) (map[string][]domain.GeoHit, map[string]*repositories.QueryInfo, error)

	// SetDebugWriter устанавливает writer для записи отладочной информации
	SetDebugWriter(writer *writers.DebugWriter)

	// GetStats возвращает статистику запросов
	GetStats() (success, failure int64)
}
