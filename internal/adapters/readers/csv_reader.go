package readers

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/hex"
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
	strictMode    bool
	debugMode     bool // новый флаг для отладки
}

// NewCSVReader создает новый CSVReader
func NewCSVReader(filePath string, delimiter rune, batchSize int, checkpointPath string, strictMode, debugMode bool) (*CSVReader, error) {
	reader := &CSVReader{
		filePath:      filePath,
		delimiter:     delimiter,
		batchSize:     batchSize,
		checkpointMgr: checkpoint.NewManager(checkpointPath),
		strictMode:    strictMode,
		debugMode:     debugMode,
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
	r.reader = csv.NewReader(bufio.NewReaderSize(r.file, 1024*1024))
	r.reader.Comma = r.delimiter
	r.reader.ReuseRecord = true
	r.reader.FieldsPerRecord = -1
	r.reader.TrimLeadingSpace = true

	// Пропускаем заголовок, если мы в начале файла
	if cp.Offset == 0 {
		if _, err := r.reader.Read(); err != nil {
			r.file.Close()
			return fmt.Errorf("failed to read header: %w", err)
		}
		r.currentLine++
		r.currentOffset, _ = r.file.Seek(0, io.SeekCurrent)
	}

	fmt.Printf("Starting from line %d (offset: %d)\n", r.currentLine+1, r.currentOffset)
	if r.debugMode {
		fmt.Println("Debug mode: ON")
	}

	return nil
}

// readRawLine читает сырую строку из файла для отладки
func (r *CSVReader) readRawLine() (string, error) {
	// Сохраняем текущую позицию
	pos, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	// Читаем строку через bufio.Reader
	reader := bufio.NewReader(r.file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	// Возвращаемся обратно
	_, err = r.file.Seek(pos, io.SeekStart)
	return line, err
}

// debugDumpLine выводит детальную информацию о строке
func (r *CSVReader) debugDumpLine(lineNum int64, err error) {
	if !r.debugMode {
		return
	}

	fmt.Printf("\n--- DEBUG: Problem at line %d ---\n", lineNum)
	fmt.Printf("Error: %v\n", err)

	// Пытаемся прочитать сырую строку
	rawLine, readErr := r.readRawLine()
	if readErr != nil {
		fmt.Printf("Failed to read raw line: %v\n", readErr)
		return
	}

	// Показываем сырую строку в разных представлениях
	fmt.Printf("Raw line length: %d bytes\n", len(rawLine))
	fmt.Printf("Raw line as string: %q\n", rawLine)
	fmt.Printf("Raw line as hex: %s\n", hex.Dump([]byte(rawLine)))

	// Показываем символы по одному
	fmt.Println("Characters:")
	for i, ch := range rawLine {
		if ch == '\n' {
			fmt.Printf("  [%d]: '\\n' (0x0A)\n", i)
		} else if ch == '\r' {
			fmt.Printf("  [%d]: '\\r' (0x0D)\n", i)
		} else if ch == '\t' {
			fmt.Printf("  [%d]: '\\t' (0x09)\n", i)
		} else if ch < 32 || ch == 127 {
			fmt.Printf("  [%d]: control char 0x%02X\n", i, ch)
		} else {
			fmt.Printf("  [%d]: '%c' (0x%02X)\n", i, ch, ch)
		}
	}
	fmt.Println("--- END DEBUG ---")
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
				// Запоминаем номер строки перед чтением
				currentLineNum := r.currentLine + 1

				// Пытаемся прочитать следующую запись
				record, err := r.readNextRecord()
				if err == io.EOF {
					if len(batch) > 0 {
						r.sendBatch(ctx, batch, recordsChan)
					}

					if err := r.saveCheckpoint(); err != nil {
						errChan <- fmt.Errorf("failed to save final checkpoint: %w", err)
					}

					fmt.Printf("Finished processing. Total records: %d, Skipped lines: %d\n",
						r.currentLine, r.skippedLines)
					return
				}

				// Обрабатываем ошибки чтения
				if err != nil {
					// Детальный вывод для отладки
					r.debugDumpLine(currentLineNum, err)

					if r.isCriticalError(err) {
						errChan <- fmt.Errorf("critical error at line %d: %w", currentLineNum, err)
						return
					}

					r.skippedLines++
					fmt.Printf("Warning: skipped line %d: %v\n", currentLineNum, err)
					continue
				}

				batch = append(batch, record)

				if len(batch) >= r.batchSize {
					if !r.sendBatch(ctx, batch, recordsChan) {
						return
					}

					if err := r.saveCheckpoint(); err != nil {
						errChan <- fmt.Errorf("failed to save checkpoint: %w", err)
						return
					}

					batch = batch[:0]

					if r.currentLine%10000 == 0 {
						fmt.Printf("Processed %d records, skipped %d lines...\n",
							r.currentLine, r.skippedLines)
					}
				}
			}
		}
	}()

	return recordsChan, errChan
}

// isCriticalError определяет, является ли ошибка критической
func (r *CSVReader) isCriticalError(err error) bool {
	if r.strictMode {
		return true
	}

	errStr := err.Error()
	// Пропускаем только известные ошибки парсинга CSV
	nonCritical := strings.Contains(errStr, "wrong number of fields") ||
		strings.Contains(errStr, "bare quote") ||
		strings.Contains(errStr, "invalid record length") ||
		strings.Contains(errStr, "line break")

	return !nonCritical
}

// readNextRecord читает и парсит следующую запись
func (r *CSVReader) readNextRecord() (domain.CSVRecord, error) {
	record, err := r.reader.Read()
	if err != nil {
		return domain.CSVRecord{}, err
	}

	r.currentOffset, _ = r.file.Seek(0, io.SeekCurrent)
	r.currentLine++

	return r.parseRecord(record)
}

// parseRecord преобразует []string в domain.CSVRecord
func (r *CSVReader) parseRecord(record []string) (domain.CSVRecord, error) {
	if len(record) < 6 {
		return domain.CSVRecord{}, fmt.Errorf("invalid record length: expected >=6, got %d, fields: %q",
			len(record), record)
	}

	// Очищаем поля от кавычек и пробелов
	cleaned := make([]string, len(record))
	for i, field := range record {
		cleaned[i] = strings.Trim(field, "\" ")
	}

	confidence, err := strconv.ParseFloat(cleaned[3], 64)
	if err != nil {
		return domain.CSVRecord{}, fmt.Errorf("invalid confidence value '%s' (original: '%s'): %w",
			cleaned[3], record[3], err)
	}

	startPos, err := strconv.Atoi(cleaned[4])
	if err != nil {
		return domain.CSVRecord{}, fmt.Errorf("invalid start_pos value '%s' (original: '%s'): %w",
			cleaned[4], record[4], err)
	}

	endPos, err := strconv.Atoi(cleaned[5])
	if err != nil {
		return domain.CSVRecord{}, fmt.Errorf("invalid end_pos value '%s' (original: '%s'): %w",
			cleaned[5], record[5], err)
	}

	return domain.CSVRecord{
		DocID:      cleaned[0],
		EntityType: cleaned[1],
		EntityText: cleaned[2],
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
