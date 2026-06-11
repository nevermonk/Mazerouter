package api

import (
	"math/rand"
	apiModels "megarouter/src/models"
	"slices"
)

type ProvidersPool struct {
	Providers    []*Provider
	PickStrategy string
}

type ModelsBatch struct {
	Models []*Model
}

func (mb ModelsBatch) ToOpenaiModelsList() apiModels.OpenaiModelsList {
	var openaiModels []apiModels.OpenaiModel
	for _, model := range mb.Models {
		openaiModels = append(openaiModels, model.ToOpenaiApiModel())
	}
	return apiModels.OpenaiModelsList{
		Object: "list",
		Data:   openaiModels,
	}
}

func (mb ModelsBatch) ToMazeModelsList() apiModels.MazeModelsList {
	var mazeModels []apiModels.MazeModel
	for _, model := range mb.Models {
		mazeModels = append(mazeModels, model.ToMazeApiModel())
	}
	return apiModels.MazeModelsList{
		Models: mazeModels,
	}
}

func (pool *ProvidersPool) GetAllModels() ModelsBatch {
	var models []*Model
	for _, provider := range pool.Providers {
		models = append(models, provider.Models...)
	}
	return ModelsBatch{
		Models: models,
	}
}

func (pool *ProvidersPool) GetModelRoute(modelId string) *Model {
	var candidates []*Model

	for _, provider := range pool.Providers {
		for _, model := range provider.Models {
			if model.Id == modelId || slices.Contains(model.Aliases, modelId) {
				candidates = append(candidates, model)
			}
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	var selected *Model
	if pool.PickStrategy == "random" {
		selected = candidates[rand.Intn(len(candidates))]
	} else {
		selected = candidates[0]
	}

	return selected
}
