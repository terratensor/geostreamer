package domain

import (
	"encoding/json"
	"time"
)

// FailedRecord представляет запись, которая не смогла обработаться
type FailedRecord struct {
	Timestamp  time.Time `json:"timestamp"`
	CSVRecord  CSVRecord `json:"csv_record"`
	Query      string    `json:"query"`
	Error      string    `json:"error"`
	Attempts   int       `json:"attempts"`
	HTTPStatus string    `json:"http_status,omitempty"`
	HTTPBody   string    `json:"http_body,omitempty"`
	WorkerID   int       `json:"worker_id"`
}

// SkippedRecord представляет запись, пропущенная из-за фильтрации
type SkippedRecord struct {
	Timestamp time.Time `json:"timestamp"`
	CSVRecord CSVRecord `json:"csv_record"`
	Reason    string    `json:"reason"` // "filtered", "not_found", etc
	WorkerID  int       `json:"worker_id"`
}

// ToJSON сериализует запись в JSON
func (f *FailedRecord) ToJSON() ([]byte, error) {
	return json.Marshal(f)
}

// ToJSON сериализует запись в JSON
func (s *SkippedRecord) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}
