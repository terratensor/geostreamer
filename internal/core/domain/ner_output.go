package domain

// NerOutput представляет только NER-информацию для doc_id
type NerOutput struct {
	DocID  string      `json:"doc_id"`
	NerLOC []NEREntity `json:"ner_loc"` // всегда массив, может быть пустым
	NerPER []NEREntity `json:"ner_per"` // всегда массив, может быть пустым
	NerORG []NEREntity `json:"ner_org"` // всегда массив, может быть пустым
}

// NewNerOutput создает новый экземпляр с инициализированными массивами
func NewNerOutput(docID string) *NerOutput {
	return &NerOutput{
		DocID:  docID,
		NerLOC: make([]NEREntity, 0),
		NerPER: make([]NEREntity, 0),
		NerORG: make([]NEREntity, 0),
	}
}
