package writers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/terratensor/geostreamer/internal/core/domain"
)

// EnrichedWriter пишет обогащенные результаты в NDJSON формате
type EnrichedWriter struct {
	file           *os.File
	writer         *bufio.Writer
	mu             sync.Mutex
	recordsWritten int64
	bytesWritten   int64
	batchesWritten int64
	flushInterval  time.Duration
	lastFlush      time.Time
}

// EnrichedWriterConfig конфигурация для enriched writer
type EnrichedWriterConfig struct {
	FilePath      string
	FlushInterval time.Duration
	BufferSize    int
}

// NewEnrichedWriter создает новый EnrichedWriter
func NewEnrichedWriter(cfg EnrichedWriterConfig) (*EnrichedWriter, error) {
	if cfg.FilePath == "" {
		return nil, fmt.Errorf("file path cannot be empty")
	}

	// Создаем директорию, если её нет
	if err := os.MkdirAll(filepath.Dir(cfg.FilePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Открываем файл (создаем или перезаписываем)
	file, err := os.Create(cfg.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create enriched file: %w", err)
	}

	// Устанавливаем размер буфера (по умолчанию 1MB)
	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = 1024 * 1024
	}

	// Устанавливаем интервал сброса (по умолчанию 5 секунд)
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}

	return &EnrichedWriter{
		file:          file,
		writer:        bufio.NewWriterSize(file, bufferSize),
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
	}, nil
}

// WriteBatch записывает пачку обогащенных результатов
func (w *EnrichedWriter) WriteBatch(ctx context.Context, results []domain.EnrichedGeoOutput) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, result := range results {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			data, err := json.Marshal(result)
			if err != nil {
				return fmt.Errorf("failed to marshal enriched result: %w", err)
			}

			n, err := w.writer.Write(data)
			if err != nil {
				return fmt.Errorf("failed to write enriched result: %w", err)
			}
			w.bytesWritten += int64(n)
			w.recordsWritten++

			// Добавляем новую строку
			if _, err := w.writer.Write([]byte{'\n'}); err != nil {
				return fmt.Errorf("failed to write newline: %w", err)
			}
			w.bytesWritten++
		}
	}

	w.batchesWritten++

	// Проверяем, не пора ли сбросить буфер
	if time.Since(w.lastFlush) > w.flushInterval {
		if err := w.flush(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
		w.lastFlush = time.Now()
	}

	return nil
}

// flush внутренний метод для сброса буфера
func (w *EnrichedWriter) flush() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Flush публичный метод для сброса буфера
func (w *EnrichedWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flush()
}

// Close закрывает writer
func (w *EnrichedWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// GetStats возвращает статистику записи
func (w *EnrichedWriter) GetStats() (records, bytes, batches int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.recordsWritten, w.bytesWritten, w.batchesWritten
}
