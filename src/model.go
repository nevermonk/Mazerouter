package api

import (
	apiModels "mazerouter/src/models"
	"time"

	"github.com/openai/openai-go/v3"
)

type Model struct {
	Id          string
	OpenaiObj   openai.Model
	ProviderRef *Provider
	Aliases     []string

	Awailable bool // влияет на участие модели в роутинге

	latency time.Time
}

func NewModel(obj openai.Model, provider *Provider, aliases []string) *Model {
	return &Model{
		Id:          obj.ID,
		OpenaiObj:   obj,
		ProviderRef: provider,
		Aliases:     aliases,
		Awailable:   true,
	}

}

func (m Model) ToOpenaiApiModel() apiModels.OpenaiModel {
	return apiModels.OpenaiModel{
		ID:       m.OpenaiObj.ID,
		Object:   string(m.OpenaiObj.Object),
		Created:  m.OpenaiObj.Created,
		Owned_by: m.OpenaiObj.OwnedBy,
	}
}

func (m Model) ToMazeApiModel() apiModels.MazeModel {
	return apiModels.MazeModel{
		Id:           m.OpenaiObj.ID,
		ProviderName: m.ProviderRef.Name,
		Aliases:      m.Aliases,
	}
}
