package checkpoint

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Manager управляет сохранением и загрузкой чекпоинтов
type Manager struct {
	filePath string
	mu       sync.RWMutex
}

// Checkpoint представляет сохраненное состояние
type Checkpoint struct {
	Offset     int64 `json:"offset"`      // смещение в файле
	LineNumber int64 `json:"line_number"` // номер строки
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
	}

	data, err := json.Marshal(checkpoint)
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

// Load загружает чекпоинт из файла
func (m *Manager) Load() (*Checkpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, err := os.ReadFile(m.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &Checkpoint{Offset: 0, LineNumber: 0}, nil
		}
		return nil, err
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, err
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
