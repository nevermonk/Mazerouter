package api

import (
	"time"

	"github.com/openai/openai-go/v3"
)

type Model struct {
	ID       string `json:"id"`
	Object   string `json:"object"`
	Created  int64  `json:"created,omitempty"`
	Owned_by string `json:"owned_by,omitempty"`

	latency time.Time
}

func NewModel(obj openai.Model) Model {
	return Model{
		ID:       obj.ID,
		Object:   string(obj.Object),
		Created:  obj.Created,
		Owned_by: obj.OwnedBy,
	}
}
