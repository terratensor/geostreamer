package domain

import (
	"strings" // Добавляем недостающий импорт
)

// GeoHit представляет результат поиска из Manticore
type GeoHit struct {
	ID              uint64   `json:"id"`
	Name            string   `json:"name"`
	GeohashesString string   `json:"geohashes_string"`
	GeohashesUint64 []uint64 `json:"geohashes_uint64"`
	Occurrences     int      `json:"occurrences"`
	FirstGeonameID  uint64   `json:"first_geoname_id"`
}

// ToGeoResult преобразует хит в часть результата агрегации
func (h *GeoHit) ToGeoResult() ([]string, []uint64) {
	// Парсим строку с геохешами
	var strings_list []string
	if h.GeohashesString != "" {
		// Разделяем по запятой
		for _, part := range strings.Split(h.GeohashesString, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				strings_list = append(strings_list, trimmed)
			}
		}
	}

	return strings_list, h.GeohashesUint64
}
