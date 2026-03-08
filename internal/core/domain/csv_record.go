package domain

// CSVRecord представляет одну запись из исходного CSV файла
type CSVRecord struct {
	DocID      string
	EntityType string
	EntityText string
	Confidence float64
	StartPos   int
	EndPos     int
	LineNum    int64 // Номер строки для отслеживания прогресса
}
