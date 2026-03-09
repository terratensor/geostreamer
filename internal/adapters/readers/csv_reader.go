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
			cp.LineNumber = 0 // Сбрасываем номер строки, так как не знаем точный
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

	// Создаем CSV reader с буфером (всегда создаем новый после seek)
	r.reader = csv.NewReader(bufio.NewReaderSize(r.file, 1024*1024))
	r.reader.Comma = r.delimiter
	r.reader.ReuseRecord = true
	r.reader.FieldsPerRecord = -1
	r.reader.TrimLeadingSpace = true

	// Если мы не в начале файла, проверяем позицию
	if cp.Offset > 0 {
		// Проверяем, что мы действительно на начале строки
		if err := r.validatePosition(); err != nil {
			log.Printf("Warning: position validation failed at offset %d: %v", cp.Offset, err)

			// Если позиция невалидна, пробуем найти следующую валидную строку
			if err := r.skipToNextLine(); err != nil {
				log.Printf("Warning: failed to skip to next line: %v", err)
			} else {
				// Обновляем offset после пропуска строки
				r.currentOffset, _ = r.file.Seek(0, io.SeekCurrent)
				log.Printf("Advanced to next line at offset %d", r.currentOffset)
			}
		} else if r.debugMode {
			log.Printf("Position %d validated successfully", cp.Offset)
		}
	}

	// Пропускаем заголовок, если мы в начале файла
	if cp.Offset == 0 {
		// Пробуем прочитать заголовок
		header, err := r.reader.Read()
		if err != nil {
			r.file.Close()
			return fmt.Errorf("failed to read header: %w", err)
		}

		// Проверяем, что это действительно заголовок
		if len(header) >= 6 {
			r.currentLine++
			r.currentOffset, _ = r.file.Seek(0, io.SeekCurrent)
			if r.debugMode {
				log.Printf("Header read successfully: %v", header)
			}
		} else {
			log.Printf("Warning: header may be invalid: %v", header)
		}
	}

	log.Printf("Starting from line %d (offset: %d)", r.currentLine+1, r.currentOffset)
	if r.debugMode {
		log.Println("Debug mode: ON")
	}

	return nil
}

// findLineStart ищет ближайшее начало строки ДО указанного offset
func (r *CSVReader) findLineStart(offset int64) (int64, error) {
	if offset == 0 {
		return 0, nil
	}

	// Отступаем немного назад, чтобы найти начало строки
	lookback := int64(1024) // 1KB

	start := offset - lookback
	if start < 0 {
		start = 0
	}

	// Читаем буфер вокруг проблемной позиции
	buffer := make([]byte, lookback)
	n, err := r.file.ReadAt(buffer, start)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read buffer at %d: %w", start, err)
	}
	buffer = buffer[:n]

	if r.debugMode {
		log.Printf("Searching for line start near offset %d (lookback %d bytes)", offset, lookback)
	}

	// Ищем последний символ новой строки в буфере
	lastNewline := -1
	for i := len(buffer) - 1; i >= 0; i-- {
		if buffer[i] == '\n' {
			lastNewline = i
			break
		}
	}

	// Если нашли новую строку
	if lastNewline >= 0 {
		newOffset := start + int64(lastNewline) + 1
		if r.debugMode && newOffset != offset {
			log.Printf("Found line start at offset %d (was %d)", newOffset, offset)
		}
		return newOffset, nil
	}

	// Если не нашли, пробуем с большим буфером
	if lookback < 1024*1024 { // Не больше 1MB
		return r.findLineStartWithLargerLookback(offset, lookback*2)
	}

	log.Printf("Warning: could not find line start near %d", offset)
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

	// Ищем последний символ новой строки
	for i := len(buffer) - 1; i >= 0; i-- {
		if buffer[i] == '\n' {
			return start + int64(i) + 1, nil
		}
	}

	return offset, nil
}

// validatePosition проверяет, что текущая позиция - начало валидной строки
func (r *CSVReader) validatePosition() error {
	// Сохраняем текущую позицию
	pos, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Пробуем прочитать первую строку
	tempReader := csv.NewReader(bufio.NewReader(r.file))
	tempReader.Comma = r.delimiter
	tempReader.FieldsPerRecord = -1
	tempReader.TrimLeadingSpace = true

	record, err := tempReader.Read()

	// Возвращаемся на исходную позицию
	if _, seekErr := r.file.Seek(pos, io.SeekStart); seekErr != nil {
		return fmt.Errorf("failed to seek back: %w", seekErr)
	}

	if err != nil {
		return fmt.Errorf("cannot read at current position: %w", err)
	}

	// Проверяем, что запись имеет правильный формат
	if len(record) < 3 {
		return fmt.Errorf("invalid record format at position %d", pos)
	}

	if r.debugMode {
		log.Printf("Position %d validated with record: %v", pos, record)
	}
	return nil
}

// skipToNextLine пропускает текущую строку и переходит к следующей
func (r *CSVReader) skipToNextLine() error {
	// Создаем buffered reader для чтения по байтам
	reader := bufio.NewReader(r.file)

	for {
		b, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Нашли конец строки
		if b == '\n' {
			break
		}
	}

	// Получаем текущую позицию после прочитанной строки
	offset, err := r.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Обновляем смещение в reader'е
	r.currentOffset = offset

	// Создаем новый CSV reader с обновленной позицией
	r.reader = csv.NewReader(bufio.NewReaderSize(r.file, 1024*1024))
	r.reader.Comma = r.delimiter
	r.reader.ReuseRecord = true
	r.reader.FieldsPerRecord = -1
	r.reader.TrimLeadingSpace = true

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
