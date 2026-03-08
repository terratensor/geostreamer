package readers

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/terratensor/geostreamer/internal/core/domain"
	"github.com/terratensor/geostreamer/pkg/checkpoint"
)

// CSVReader реализует интерфейс RecordSource для CSV файлов
type CSVReader struct {
	filePath      string
	delimiter     rune
	batchSize     int
	checkpointMgr *checkpoint.Manager
	file          *os.File
	reader        *csv.Reader
	currentLine   int64
	currentOffset int64
	skippedLines  int64
	strictMode    bool // если true - останавливаться при ошибках, false - пропускать
}

// NewCSVReader создает новый CSVReader
func NewCSVReader(filePath string, delimiter rune, batchSize int, checkpointPath string, strictMode bool) (*CSVReader, error) {
	reader := &CSVReader{
		filePath:      filePath,
		delimiter:     delimiter,
		batchSize:     batchSize,
		checkpointMgr: checkpoint.NewManager(checkpointPath),
		strictMode:    strictMode,
		skippedLines:  0,
	}

	if err := reader.init(); err != nil {
		return nil, err
	}

	return reader, nil
}

// init открывает файл и восстанавливает позицию из чекпоинта
func (r *CSVReader) init() error {
	// Загружаем чекпоинт
	cp, err := r.checkpointMgr.Load()
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	// Открываем файл
	file, err := os.Open(r.filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	r.file = file

	// Если есть сохраненное смещение, переходим на него
	if cp.Offset > 0 {
		_, err = r.file.Seek(cp.Offset, io.SeekStart)
		if err != nil {
			r.file.Close()
			return fmt.Errorf("failed to seek to offset %d: %w", cp.Offset, err)
		}
		r.currentOffset = cp.Offset
		r.currentLine = cp.LineNumber
	}

	// Создаем CSV reader с буфером
	r.reader = csv.NewReader(bufio.NewReaderSize(r.file, 1024*1024)) // 1MB буфер
	r.reader.Comma = r.delimiter
	r.reader.ReuseRecord = true      // Важно для производительности!
	r.reader.FieldsPerRecord = -1    // Разрешаем переменное количество полей
	r.reader.TrimLeadingSpace = true // Обрезаем пробелы

	// Пропускаем заголовок, если мы в начале файла
	if cp.Offset == 0 {
		if _, err := r.reader.Read(); err != nil {
			r.file.Close()
			return fmt.Errorf("failed to read header: %w", err)
		}
		r.currentLine++
		r.currentOffset, _ = r.file.Seek(0, io.SeekCurrent)
	}

	// Логируем информацию о старте
	fmt.Printf("Starting from line %d (offset: %d)\n", r.currentLine+1, r.currentOffset)

	return nil
}

// ReadRecords читает записи и отправляет их в канал
func (r *CSVReader) ReadRecords(ctx context.Context) (<-chan domain.CSVRecord, <-chan error) {
	recordsChan := make(chan domain.CSVRecord, r.batchSize)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordsChan)
		defer close(errChan)

		batch := make([]domain.CSVRecord, 0, r.batchSize)

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				// Пытаемся прочитать следующую запись
				record, err := r.readNextRecord()
				if err == io.EOF {
					// Отправляем последний батч, если он не пустой
					if len(batch) > 0 {
						r.sendBatch(ctx, batch, recordsChan)
					}

					// Сохраняем финальный чекпоинт
					if err := r.saveCheckpoint(); err != nil {
						errChan <- fmt.Errorf("failed to save final checkpoint: %w", err)
					}

					fmt.Printf("Finished processing. Total records: %d, Skipped lines: %d\n",
						r.currentLine, r.skippedLines)
					return
				}

				// Обрабатываем ошибки чтения
				if err != nil {
					// Проверяем, является ли ошибка критической
					if r.isCriticalError(err) {
						errChan <- fmt.Errorf("critical error at line ~%d: %w", r.currentLine+1, err)
						return
					}

					// Пропускаем битую строку
					r.skippedLines++
					fmt.Printf("Warning: skipped line %d (invalid format): %v\n", r.currentLine+1, err)
					continue
				}

				// Валидная запись - добавляем в батч
				batch = append(batch, record)

				// Если набрали батч, отправляем
				if len(batch) >= r.batchSize {
					if !r.sendBatch(ctx, batch, recordsChan) {
						return
					}

					// Сохраняем чекпоинт после каждого батча
					if err := r.saveCheckpoint(); err != nil {
						errChan <- fmt.Errorf("failed to save checkpoint: %w", err)
						return
					}

					// Очищаем батч
					batch = batch[:0]

					// Логируем прогресс
					fmt.Printf("Processed %d records, skipped %d lines...\n",
						r.currentLine, r.skippedLines)
				}
			}
		}
	}()

	return recordsChan, errChan
}

// isCriticalError определяет, является ли ошибка критической
func (r *CSVReader) isCriticalError(err error) bool {
	// В строгом режиме все ошибки критические
	if r.strictMode {
		return true
	}

	// Определяем типы ошибок, которые можно пропустить
	errStr := err.Error()
	return !strings.Contains(errStr, "wrong number of fields") &&
		!strings.Contains(errStr, "bare quote") &&
		!strings.Contains(errStr, "invalid record length") &&
		!strings.Contains(errStr, "line break")
}

// readNextRecord читает и парсит следующую запись
func (r *CSVReader) readNextRecord() (domain.CSVRecord, error) {
	record, err := r.reader.Read()
	if err != nil {
		return domain.CSVRecord{}, err
	}

	// Обновляем смещение и номер строки
	r.currentOffset, _ = r.file.Seek(0, io.SeekCurrent)
	r.currentLine++

	// Парсим запись
	return r.parseRecord(record)
}

// parseRecord преобразует []string в domain.CSVRecord
func (r *CSVReader) parseRecord(record []string) (domain.CSVRecord, error) {
	if len(record) < 6 {
		return domain.CSVRecord{}, fmt.Errorf("invalid record length: expected >=6, got %d", len(record))
	}

	// Очищаем поля от кавычек и пробелов
	for i := range record {
		record[i] = strings.Trim(record[i], "\" ")
	}

	confidence, err := strconv.ParseFloat(record[3], 64)
	if err != nil {
		return domain.CSVRecord{}, fmt.Errorf("invalid confidence value '%s': %w", record[3], err)
	}

	startPos, err := strconv.Atoi(record[4])
	if err != nil {
		return domain.CSVRecord{}, fmt.Errorf("invalid start_pos value '%s': %w", record[4], err)
	}

	endPos, err := strconv.Atoi(record[5])
	if err != nil {
		return domain.CSVRecord{}, fmt.Errorf("invalid end_pos value '%s': %w", record[5], err)
	}

	return domain.CSVRecord{
		DocID:      strings.TrimSpace(record[0]),
		EntityType: strings.TrimSpace(record[1]),
		EntityText: strings.TrimSpace(record[2]),
		Confidence: confidence,
		StartPos:   startPos,
		EndPos:     endPos,
		LineNum:    r.currentLine,
	}, nil
}

// sendBatch отправляет батч записей в канал
func (r *CSVReader) sendBatch(ctx context.Context, batch []domain.CSVRecord, recordsChan chan<- domain.CSVRecord) bool {
	for _, record := range batch {
		select {
		case <-ctx.Done():
			return false
		case recordsChan <- record:
		}
	}
	return true
}

// saveCheckpoint сохраняет текущий прогресс
func (r *CSVReader) saveCheckpoint() error {
	return r.checkpointMgr.Save(r.currentOffset, r.currentLine)
}

// GetProgress возвращает текущий прогресс
func (r *CSVReader) GetProgress() int64 {
	return r.currentLine
}

// GetSkippedLines возвращает количество пропущенных строк
func (r *CSVReader) GetSkippedLines() int64 {
	return r.skippedLines
}

// Close закрывает файл
func (r *CSVReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
