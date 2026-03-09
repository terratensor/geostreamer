package domain

import (
	"encoding/json"
	"time"
)

// QueryDebugInfo полная информация о запросе для отладки
type QueryDebugInfo struct {
	Text       string        `json:"text"`
	Query      string        `json:"query"`
	Error      string        `json:"error,omitempty"`
	HTTPStatus string        `json:"http_status,omitempty"`
	HTTPBody   string        `json:"http_body,omitempty"`
	Attempts   int           `json:"attempts"`
	WorkerID   int           `json:"worker_id"`
	Duration   time.Duration `json:"duration_ms"`
	Timestamp  time.Time     `json:"timestamp"`
	HitCount   int           `json:"hit_count"`
	Cached     bool          `json:"cached,omitempty"`
	Response   string        `json:"response,omitempty"`
}

// SkippedRecord представляет запись, пропущенная из-за фильтрации или отсутствия в Manticore
type SkippedRecord struct {
	Timestamp time.Time       `json:"timestamp"`
	CSVRecord CSVRecord       `json:"csv_record"`
	Reason    string          `json:"reason"` // "filtered", "not_found_in_manticore", "error", "hit_without_geohashes"
	QueryInfo *QueryDebugInfo `json:"query_info,omitempty"`
	WorkerID  int             `json:"worker_id"`
}

// FailedRecord представляет запись, которая не смогла обработаться
type FailedRecord struct {
	Timestamp time.Time       `json:"timestamp"`
	CSVRecord CSVRecord       `json:"csv_record"`
	QueryInfo *QueryDebugInfo `json:"query_info"`
	WorkerID  int             `json:"worker_id"`
}

// ToJSON сериализует запись в JSON
func (f *FailedRecord) ToJSON() ([]byte, error) {
	return json.Marshal(f)
}

// ToJSON сериализует запись в JSON
func (s *SkippedRecord) ToJSON() ([]byte, error) {
	return json.Marshal(s)
}
