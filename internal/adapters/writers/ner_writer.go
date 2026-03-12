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

// NerWriter пишет NER-only результаты в NDJSON формате
type NerWriter struct {
	file           *os.File
	writer         *bufio.Writer
	mu             sync.Mutex
	recordsWritten int64
	bytesWritten   int64
	batchesWritten int64
	flushInterval  time.Duration
	lastFlush      time.Time
}

// NerWriterConfig конфигурация для NerWriter
type NerWriterConfig struct {
	FilePath      string
	FlushInterval time.Duration
	BufferSize    int
}

// NewNerWriter создает новый NerWriter
func NewNerWriter(cfg NerWriterConfig) (*NerWriter, error) {
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
		return nil, fmt.Errorf("failed to create ner file: %w", err)
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

	return &NerWriter{
		file:          file,
		writer:        bufio.NewWriterSize(file, bufferSize),
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
	}, nil
}

// WriteBatch записывает пачку NER-only результатов
func (w *NerWriter) WriteBatch(ctx context.Context, results []domain.NerOutput) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, result := range results {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			data, err := json.Marshal(result)
			if err != nil {
				return fmt.Errorf("failed to marshal ner result: %w", err)
			}

			n, err := w.writer.Write(data)
			if err != nil {
				return fmt.Errorf("failed to write ner result: %w", err)
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
func (w *NerWriter) flush() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Flush публичный метод для сброса буфера
func (w *NerWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flush()
}

// Close закрывает writer
func (w *NerWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// GetStats возвращает статистику записи
func (w *NerWriter) GetStats() (records, bytes, batches int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.recordsWritten, w.bytesWritten, w.batchesWritten
}
