package domain

// GeoResult представляет агрегированный результат для одного doc_id
type GeoResult struct {
	DocID              string
	GeohashesStringMap map[string]struct{}
	GeohashesUint64Map map[uint64]struct{}
}

// NewGeoResult создает новый GeoResult с инициализированными картами
func NewGeoResult(docID string) *GeoResult {
	return &GeoResult{
		DocID:              docID,
		GeohashesStringMap: make(map[string]struct{}),
		GeohashesUint64Map: make(map[uint64]struct{}),
	}
}

// ToOutput преобразует внутренние карты в плоские массивы для вывода
func (r *GeoResult) ToOutput() GeoOutput {
	strings := make([]string, 0, len(r.GeohashesStringMap))
	for s := range r.GeohashesStringMap {
		strings = append(strings, s)
	}

	uints := make([]uint64, 0, len(r.GeohashesUint64Map))
	for u := range r.GeohashesUint64Map {
		uints = append(uints, u)
	}

	return GeoOutput{
		DocID:           r.DocID,
		GeohashesString: strings,
		GeohashesUint64: uints,
	}
}

// GeoOutput структура для сериализации в JSON
type GeoOutput struct {
	DocID           string   `json:"doc_id"`
	GeohashesString []string `json:"geohashes_string"`
	GeohashesUint64 []uint64 `json:"geohashes_uint64"`
}
