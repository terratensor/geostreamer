package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger структурированный логгер
type Logger struct {
	level  Level
	logger *log.Logger
	mu     sync.Mutex
	prefix string
}

var (
	globalLogger *Logger
	once         sync.Once
)

// Init инициализирует глобальный логгер
func Init(levelStr, outputFile string) error {
	var level Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = LevelDebug
	case "info":
		level = LevelInfo
	case "warn":
		level = LevelWarn
	case "error":
		level = LevelError
	default:
		level = LevelInfo
	}

	var writer io.Writer = os.Stdout
	if outputFile != "" {
		file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		writer = file
	}

	once.Do(func() {
		globalLogger = &Logger{
			level:  level,
			logger: log.New(writer, "", log.LstdFlags),
		}
	})

	return nil
}

// Get возвращает глобальный логгер
func Get() *Logger {
	if globalLogger == nil {
		Init("info", "")
	}
	return globalLogger
}

// WithPrefix создает новый логгер с префиксом
func (l *Logger) WithPrefix(prefix string) *Logger {
	return &Logger{
		level:  l.level,
		logger: l.logger,
		prefix: l.prefix + " " + prefix,
	}
}

// log выводит сообщение с указанным уровнем
func (l *Logger) log(level Level, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	msg := fmt.Sprintf(format, args...)
	prefix := fmt.Sprintf("[%s]", level.String())
	if l.prefix != "" {
		prefix = prefix + l.prefix
	}
	l.logger.Println(prefix, msg)
}

// Debug выводит отладочное сообщение
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(LevelDebug, format, args...)
}

// Info выводит информационное сообщение
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(LevelInfo, format, args...)
}

// Warn выводит предупреждение
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(LevelWarn, format, args...)
}

// Error выводит сообщение об ошибке
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(LevelError, format, args...)
}

// Stats структура для статистики
type Stats struct {
	ProcessedRecords int64
	SkippedRecords   int64
	WrittenRecords   int64
	ManticoreSuccess int64
	ManticoreFailure int64
	BytesWritten     int64
	ProcessingTime   time.Duration
	RecordsPerSecond float64
}

// PrintStats выводит статистику в структурированном виде
func (l *Logger) PrintStats(stats Stats) {
	l.Info("=== STATISTICS ===")
	l.Info("Processed: %d records", stats.ProcessedRecords)
	l.Info("Skipped: %d records", stats.SkippedRecords)
	l.Info("Written: %d records", stats.WrittenRecords)
	l.Info("Bytes written: %d", stats.BytesWritten)
	l.Info("Manticore: %d success, %d failures", stats.ManticoreSuccess, stats.ManticoreFailure)

	if stats.ProcessingTime > 0 {
		rate := float64(stats.ProcessedRecords) / stats.ProcessingTime.Seconds()
		l.Info("Rate: %.2f records/sec", rate)
	}
	l.Info("=================")
}
