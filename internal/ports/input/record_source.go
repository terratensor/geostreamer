package input

import (
	"context"

	"github.com/terratensor/geostreamer/internal/core/domain"
)

// RecordSource определяет интерфейс для любого источника CSV записей
type RecordSource interface {
	// ReadRecords возвращает канал с батчами записей
	ReadRecords(ctx context.Context) (<-chan []domain.CSVRecord, <-chan error)

	// GetProgress возвращает текущий прогресс (смещение в файле)
	GetProgress() int64

	// Close закрывает источник
	Close() error
}
