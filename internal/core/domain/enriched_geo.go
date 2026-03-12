package domain

// NEREntity представляет детальную информацию об одной именованной сущности
type NEREntity struct {
	Value      string   `json:"value"`
	StartPos   int      `json:"start_pos"`
	EndPos     int      `json:"end_pos"`
	Geohash    []string `json:"geohash"` // всегда массив, может быть пустым
	Confidence float64  `json:"confidence"`
}

// EnrichedGeoOutput расширенная версия GeoOutput с NER-информацией
type EnrichedGeoOutput struct {
	DocID           string      `json:"doc_id"`
	GeohashesString []string    `json:"geohashes_string"` // массив, как в GeoOutput
	GeohashesUint64 []uint64    `json:"geohashes_uint64"` // массив, как в GeoOutput
	NerLOC          []NEREntity `json:"ner_loc"`          // всегда присутствует
	NerPER          []NEREntity `json:"ner_per"`          // всегда присутствует
	NerORG          []NEREntity `json:"ner_org"`          // всегда присутствует
}

// NewEnrichedGeoOutput создает новый экземпляр с инициализированными массивами
func NewEnrichedGeoOutput(docID string) *EnrichedGeoOutput {
	return &EnrichedGeoOutput{
		DocID:           docID,
		GeohashesString: make([]string, 0),
		GeohashesUint64: make([]uint64, 0),
		NerLOC:          make([]NEREntity, 0),
		NerPER:          make([]NEREntity, 0),
		NerORG:          make([]NEREntity, 0),
	}
}

// FromGeoOutput создает EnrichedGeoOutput на основе существующего GeoOutput
func (e *EnrichedGeoOutput) FromGeoOutput(geo GeoOutput) {
	e.DocID = geo.DocID
	e.GeohashesString = geo.GeohashesString
	e.GeohashesUint64 = geo.GeohashesUint64
}
