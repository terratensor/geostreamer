package checkpoint

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/terratensor/geostreamer/internal/version"
)

// Manager управляет сохранением и загрузкой чекпоинтов
type Manager struct {
	filePath string
	mu       sync.RWMutex
}

// Checkpoint представляет сохраненное состояние
type Checkpoint struct {
	Offset       int64     `json:"offset"`        // смещение в файле
	LineNumber   int64     `json:"line_number"`   // номер строки
	SkippedLines int64     `json:"skipped_lines"` // пропущено строк
	Timestamp    time.Time `json:"timestamp"`     // время сохранения
	Version      string    `json:"version"`       // версия программы
}

// NewManager создает новый менеджер чекпоинтов
func NewManager(filePath string) *Manager {
	return &Manager{
		filePath: filePath,
	}
}

// Save сохраняет чекпоинт в файл
func (m *Manager) Save(offset, lineNum int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Создаем директорию, если её нет
	if err := os.MkdirAll(filepath.Dir(m.filePath), 0755); err != nil {
		return err
	}

	checkpoint := Checkpoint{
		Offset:     offset,
		LineNumber: lineNum,
		Timestamp:  time.Now(),
		Version:    version.Short(), // используем текущую версию
	}

	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}

	// Атомарно записываем во временный файл и переименовываем
	tmpFile := m.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpFile, m.filePath)
}

// Load загружает чекпоинт и проверяет совместимость версий
func (m *Manager) Load() (*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, err := os.ReadFile(m.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &Checkpoint{
				Offset:     0,
				LineNumber: 0,
				Timestamp:  time.Now(),
				Version:    version.Short(),
			}, nil
		}
		return nil, err
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
	}

	// Проверяем совместимость версий (опционально)
	if checkpoint.Version != version.Short() {
		// Можно добавить предупреждение, но не блокировать
		// log.Printf("Warning: checkpoint created with version %s, current version is %s",
		//    checkpoint.Version, version.Short())
	}

	return &checkpoint, nil
}

// Clear удаляет файл чекпоинта
func (m *Manager) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := os.Remove(m.filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// GetPath возвращает путь к файлу чекпоинта
func (m *Manager) GetPath() string {
	return m.filePath
}
