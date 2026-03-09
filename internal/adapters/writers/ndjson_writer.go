package writers

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/terratensor/geostreamer/internal/core/domain"
	"github.com/terratensor/geostreamer/internal/ports/output"
)

// NDJSONWriter пишет результаты в NDJSON формате (одна JSON строка на запись)
type NDJSONWriter struct {
	file          *os.File
	writer        *bufio.Writer
	gzipWriter    *gzip.Writer
	mu            sync.Mutex
	stats         output.WriterStats
	flushInterval time.Duration
	lastFlush     time.Time
	useGzip       bool
}

// NDJSONWriterConfig конфигурация для NDJSON writer
type NDJSONWriterConfig struct {
	FilePath      string        // путь к выходному файлу
	FlushInterval time.Duration // как часто сбрасывать буфер на диск
	UseGzip       bool          // сжимать ли выходной файл (добавит .gz)
	BufferSize    int           // размер буфера в байтах (по умолчанию 1MB)
}

// NewNDJSONWriter создает новый NDJSON writer
func NewNDJSONWriter(cfg NDJSONWriterConfig) (*NDJSONWriter, error) {
	// Определяем путь к файлу с учетом gzip
	filePath := cfg.FilePath
	if cfg.UseGzip && !strings.HasSuffix(filePath, ".gz") {
		filePath = filePath + ".gz"
	}

	// Создаем директорию, если её нет
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Открываем файл
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file: %w", err)
	}

	// Устанавливаем размер буфера (по умолчанию 1MB)
	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = 1024 * 1024 // 1MB
	}

	bufWriter := bufio.NewWriterSize(file, bufferSize)

	var gzipWriter *gzip.Writer
	if cfg.UseGzip {
		gzipWriter = gzip.NewWriter(bufWriter)
	}

	// Устанавливаем интервал сброса (по умолчанию 5 секунд)
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}

	return &NDJSONWriter{
		file:          file,
		writer:        bufWriter,
		gzipWriter:    gzipWriter,
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
		useGzip:       cfg.UseGzip,
		stats:         output.WriterStats{},
	}, nil
}

// WriteBatch записывает пачку результатов
func (w *NDJSONWriter) WriteBatch(ctx context.Context, results []domain.GeoOutput) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, result := range results {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := w.writeResult(result); err != nil {
				return fmt.Errorf("failed to write result: %w", err)
			}
			w.stats.RecordsWritten++
		}
	}

	w.stats.BatchesWritten++

	// Проверяем, не пора ли сбросить буфер
	if time.Since(w.lastFlush) > w.flushInterval {
		if err := w.flush(); err != nil {
			return fmt.Errorf("failed to flush buffer: %w", err)
		}
		w.lastFlush = time.Now()
	}

	return nil
}

// writeResult записывает один результат
func (w *NDJSONWriter) writeResult(result domain.GeoOutput) error {
	data, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	// Выбираем куда писать - в gzip или напрямую
	var writer io.Writer
	if w.gzipWriter != nil {
		writer = w.gzipWriter
	} else {
		writer = w.writer
	}

	n, err := writer.Write(data)
	if err != nil {
		return err
	}
	w.stats.BytesWritten += int64(n)

	// Добавляем новую строку
	n, err = writer.Write([]byte{'\n'})
	if err != nil {
		return err
	}
	w.stats.BytesWritten += int64(n)

	return nil
}

// Flush сбрасывает буфер на диск
func (w *NDJSONWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flush()
}

// flush внутренний метод для сброса буфера
func (w *NDJSONWriter) flush() error {
	var err error

	if w.gzipWriter != nil {
		if err = w.gzipWriter.Flush(); err != nil {
			return err
		}
	}

	if err = w.writer.Flush(); err != nil {
		return err
	}

	// Синхронизируем с диском
	if err = w.file.Sync(); err != nil {
		return err
	}

	w.lastFlush = time.Now()
	return nil
}

// Close закрывает writer
func (w *NDJSONWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Сначала сбрасываем все буферы
	if w.gzipWriter != nil {
		if err := w.gzipWriter.Close(); err != nil {
			return err
		}
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	// Синхронизируем с диском
	if err := w.file.Sync(); err != nil {
		return err
	}

	// Закрываем файл
	return w.file.Close()
}

// GetStats возвращает статистику записи
func (w *NDJSONWriter) GetStats() output.WriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}
