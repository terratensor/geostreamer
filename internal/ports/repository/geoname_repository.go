package repository

import (
	"context"

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
