package output

import (
	"context"

	"github.com/terratensor/geostreamer/internal/core/domain"
)

// ResultWriter определяет интерфейс для записи результатов
type ResultWriter interface {
	// WriteBatch записывает пачку результатов
	WriteBatch(ctx context.Context, results []domain.GeoOutput) error

	// Flush сбрасывает буфер на диск
	Flush() error

	// Close закрывает writer
	Close() error

	// GetStats возвращает статистику записи
	GetStats() WriterStats
}

// WriterStats статистика записи
type WriterStats struct {
	RecordsWritten int64
	BytesWritten   int64
	BatchesWritten int64
}
