package readers

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log"
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
	maxBatchSize  int // максимальное количество записей в батче
	minBatchSize  int // минимальное количество записей для отправки
	checkpointMgr *checkpoint.Manager
	file          *os.File
	reader        *csv.Reader
	currentLine   int64
	currentOffset int64
	skippedLines  int64
	strictMode    bool
	debugMode     bool
	headerRead    bool // true если заголовок был прочитан в этом запуске
}

// NewCSVReader создает новый CSVReader
func NewCSVReader(filePath string, delimiter rune, maxBatchSize, minBatchSize int,
	checkpointPath string, strictMode, debugMode bool) (*CSVReader, error) {

	reader := &CSVReader{
		filePath:      filePath,
		delimiter:     delimiter,
		maxBatchSize:  maxBatchSize,
		minBatchSize:  minBatchSize,
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

	// Получаем размер файла для валидации
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	// Валидация чекпоинта
	if cp.Offset > fileInfo.Size() {
		log.Printf("Warning: checkpoint offset %d exceeds file size %d, resetting to 0",
			cp.Offset, fileInfo.Size())
		cp.Offset = 0
		cp.LineNumber = 0
	}

	// Если есть сохраненное смещение, переходим на него
	if cp.Offset > 0 {
		// Ищем ближайшее начало строки ДО этого смещения
		correctOffset, err := r.findLineStart(cp.Offset)
		if err != nil {
			log.Printf("Warning: error finding line start: %v", err)
			correctOffset = cp.Offset
		}

		// Если смещение изменилось, корректируем
		if correctOffset != cp.Offset {
			log.Printf("Checkpoint offset corrected: %d -> %d (line number reset)",
				cp.Offset, correctOffset)
			cp.Offset = correctOffset
			cp.LineNumber = 0
		}

		// Переходим на скорректированную позицию
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

	// Если мы не в начале файла, проверяем позицию
	if cp.Offset > 0 {
		if err := r.validatePosition(); err != nil {
			log.Printf("Warning: position validation failed at offset %d: %v", cp.Offset, err)
		}
	}

	// Пропускаем заголовок, если мы в начале файла
	if cp.Offset == 0 {
		if _, err := r.reader.Read(); err != nil {
			r.file.Close()
			return fmt.Errorf("failed to read header: %w", err)
		}
		r.currentLine++
		r.currentOffset, _ = r.file.Seek(0, io.SeekCurrent)
		r.headerRead = true // ← устанавливаем флаг
		if r.debugMode {
			log.Println("Header read successfully")
		}
	} else {
		// При возобновлении с чекпоинта заголовок не читаем
		r.headerRead = false
	}

	log.Printf("Starting from line %d (offset: %d)", r.currentLine+1, r.currentOffset)
	if r.debugMode {
		log.Println("Debug mode: ON")
	}

	return nil
}

// ReadRecords читает записи и отправляет их батчами в канал
func (r *CSVReader) ReadRecords(ctx context.Context) (<-chan []domain.CSVRecord, <-chan error) {
	batchChan := make(chan []domain.CSVRecord, 10) // буфер для батчей
	errChan := make(chan error, 1)

	go func() {
		defer close(batchChan)
		defer close(errChan)

		var currentBatch []domain.CSVRecord
		var currentDocID string
		batchSize := 0

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				// Читаем следующую запись
				record, err := r.readNextRecord()
				if err == io.EOF {
					// Отправляем последний батч, если он не пустой
					if len(currentBatch) > 0 {
						batchChan <- currentBatch
					}

					// Сохраняем финальный чекпоинт
					if err := r.saveCheckpoint(); err != nil {
						errChan <- fmt.Errorf("failed to save final checkpoint: %w", err)
					}

					// Вычисляем количество записей данных (исключая заголовок)
					totalDataRecords := r.currentLine
					if r.headerRead {
						// Если заголовок был прочитан в этом запуске, вычитаем его
						totalDataRecords--
					}

					log.Printf("Finished processing. Total records: %d, Skipped lines: %d\n",
						totalDataRecords, r.skippedLines)
					return
				}

				// Обрабатываем ошибки чтения
				if err != nil {
					r.debugDumpLine(r.currentLine+1, err)
					if r.isCriticalError(err) {
						errChan <- fmt.Errorf("critical error at line %d: %w", r.currentLine+1, err)
						return
					}
					r.skippedLines++
					log.Printf("Warning: skipped line %d: %v", r.currentLine+1, err)
					continue
				}

				// Проверяем смену doc_id
				if record.DocID != currentDocID && len(currentBatch) > 0 {
					// Отправляем батч, если набрали минимум ИЛИ принудительно по смене doc_id
					if batchSize >= r.minBatchSize {
						batchChan <- currentBatch
						currentBatch = nil
						batchSize = 0
					} else {
						// Если мало записей, но сменился doc_id - все равно отправляем,
						// чтобы не копить слишком долго
						batchChan <- currentBatch
						currentBatch = nil
						batchSize = 0
					}
				}

				// Добавляем запись в текущий батч
				currentDocID = record.DocID
				currentBatch = append(currentBatch, record)
				batchSize++

				// Проверяем, не превысили ли максимальный размер батча
				if batchSize >= r.maxBatchSize {
					// Достигли лимита - отправляем, даже если doc_id не закончился
					// (но таких документов мало, макс 58 записей)
					batchChan <- currentBatch
					currentBatch = nil
					batchSize = 0
				}
			}
		}
	}()

	return batchChan, errChan
}

// readRawLine читает сырую строку из файла для отладки
func (r *CSVReader) readRawLine() (string, error) {
	pos, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(r.file)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}

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

	rawLine, readErr := r.readRawLine()
	if readErr != nil {
		fmt.Printf("Failed to read raw line: %v\n", readErr)
		return
	}

	fmt.Printf("Raw line length: %d bytes\n", len(rawLine))
	fmt.Printf("Raw line as string: %q\n", rawLine)
	fmt.Printf("Raw line as hex: %s\n", hex.Dump([]byte(rawLine)))

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

// isCriticalError определяет, является ли ошибка критической
func (r *CSVReader) isCriticalError(err error) bool {
	if r.strictMode {
		return true
	}

	errStr := err.Error()
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

// findLineStart ищет ближайшее начало строки ДО указанного offset
func (r *CSVReader) findLineStart(offset int64) (int64, error) {
	if offset == 0 {
		return 0, nil
	}

	lookback := int64(1024)
	start := offset - lookback
	if start < 0 {
		start = 0
	}

	buffer := make([]byte, lookback)
	n, err := r.file.ReadAt(buffer, start)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read buffer at %d: %w", start, err)
	}
	buffer = buffer[:n]

	lastNewline := -1
	for i := len(buffer) - 1; i >= 0; i-- {
		if buffer[i] == '\n' {
			lastNewline = i
			break
		}
	}

	if lastNewline >= 0 {
		return start + int64(lastNewline) + 1, nil
	}

	if lookback < 1024*1024 {
		return r.findLineStartWithLargerLookback(offset, lookback*2)
	}

	return offset, nil
}

// findLineStartWithLargerLookback пробует найти начало строки с увеличенным буфером
func (r *CSVReader) findLineStartWithLargerLookback(offset, lookback int64) (int64, error) {
	start := offset - lookback
	if start < 0 {
		start = 0
	}

	buffer := make([]byte, lookback)
	n, err := r.file.ReadAt(buffer, start)
	if err != nil && err != io.EOF {
		return offset, nil
	}
	buffer = buffer[:n]

	for i := len(buffer) - 1; i >= 0; i-- {
		if buffer[i] == '\n' {
			return start + int64(i) + 1, nil
		}
	}

	return offset, nil
}

// validatePosition проверяет, что текущая позиция - начало валидной строки
func (r *CSVReader) validatePosition() error {
	pos, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	tempReader := csv.NewReader(bufio.NewReader(r.file))
	tempReader.Comma = r.delimiter
	tempReader.FieldsPerRecord = -1
	tempReader.TrimLeadingSpace = true

	record, err := tempReader.Read()

	if _, seekErr := r.file.Seek(pos, io.SeekStart); seekErr != nil {
		return fmt.Errorf("failed to seek back: %w", seekErr)
	}

	if err != nil {
		return fmt.Errorf("cannot read at current position: %w", err)
	}

	if len(record) < 3 {
		return fmt.Errorf("invalid record format at position %d", pos)
	}

	return nil
}
