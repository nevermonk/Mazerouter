package api

type ProvidersPool struct {
	Providers []*Provider
}

type ModelsResponse struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}

func (pool *ProvidersPool) GetAllModels() ModelsResponse {
	var models []Model
	for _, provider := range pool.Providers {
		models = append(models, provider.State.Models...)
	}
	return ModelsResponse{
		Object: "list",
		Data:   models,
	}
}

func (pool *ProvidersPool) FindByModel(modelID string) *Provider {
	for _, provider := range pool.Providers {
		for _, model := range provider.State.Models {
			if model.ID == modelID {
				return provider
			}
		}
	}
	return nil
}
