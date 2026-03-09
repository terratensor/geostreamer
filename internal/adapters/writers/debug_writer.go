package writers

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/terratensor/geostreamer/internal/core/domain"
)

// DebugWriter пишет отладочную информацию в NDJSON
type DebugWriter struct {
	failuresFile   *os.File
	skippedFile    *os.File
	failuresWriter *bufio.Writer
	skippedWriter  *bufio.Writer
	mu             sync.Mutex
}

// NewDebugWriter создает новый DebugWriter
func NewDebugWriter(failuresPath, skippedPath string) (*DebugWriter, error) {
	// Создаем директории если нужно
	if err := os.MkdirAll(filepath.Dir(failuresPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create failures directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(skippedPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create skipped directory: %w", err)
	}

	// Открываем файлы в режиме добавления
	failuresFile, err := os.OpenFile(failuresPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open failures file: %w", err)
	}

	skippedFile, err := os.OpenFile(skippedPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		failuresFile.Close()
		return nil, fmt.Errorf("failed to open skipped file: %w", err)
	}

	return &DebugWriter{
		failuresFile:   failuresFile,
		skippedFile:    skippedFile,
		failuresWriter: bufio.NewWriterSize(failuresFile, 1024*1024),
		skippedWriter:  bufio.NewWriterSize(skippedFile, 1024*1024),
	}, nil
}

// WriteFailed записывает информацию о неудачном запросе
func (w *DebugWriter) WriteFailed(record *domain.FailedRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	if _, err := w.failuresWriter.Write(data); err != nil {
		return err
	}
	if _, err := w.failuresWriter.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// WriteSkipped записывает информацию о пропущенной записи
func (w *DebugWriter) WriteSkipped(record *domain.SkippedRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}

	if _, err := w.skippedWriter.Write(data); err != nil {
		return err
	}
	if _, err := w.skippedWriter.Write([]byte{'\n'}); err != nil {
		return err
	}

	return nil
}

// Flush сбрасывает буферы
func (w *DebugWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.failuresWriter.Flush(); err != nil {
		return err
	}
	if err := w.skippedWriter.Flush(); err != nil {
		return err
	}
	return nil
}

// Close закрывает файлы
func (w *DebugWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.failuresWriter.Flush(); err != nil {
		return err
	}
	if err := w.skippedWriter.Flush(); err != nil {
		return err
	}
	if err := w.failuresFile.Sync(); err != nil {
		return err
	}
	if err := w.skippedFile.Sync(); err != nil {
		return err
	}
	if err := w.failuresFile.Close(); err != nil {
		return err
	}
	return w.skippedFile.Close()
}
