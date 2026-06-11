package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/openai/openai-go/v3"
	"go.uber.org/zap"
)

func HandleMazeModelsList(pool *ProvidersPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		models := pool.GetAllModels().ToMazeModelsList()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(models)
	}
}

// Openai API methods

func HandleOpenaiModelsList(pool *ProvidersPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		models := pool.GetAllModels().ToOpenaiModelsList()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(models)
	}
}

func HandleOpenaiCompletions(pool *ProvidersPool, logger *zap.SugaredLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Errorf("Failed to read request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var openaiRequest openai.ChatCompletionNewParams
		if err := json.Unmarshal(bodyBytes, &openaiRequest); err != nil {
			logger.Errorf("Failed to decode request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		logger.Infow("Incoming completions request",
			zap.String("model", openaiRequest.Model),
		)

		pickedProviderModel := pool.GetModelRoute(openaiRequest.Model)

		if pickedProviderModel == nil {
			http.Error(w, "No provider found for model", http.StatusNotFound)
			return
		}

		logger.Infof("Pick model %s from provider %s", pickedProviderModel.Id, pickedProviderModel.ProviderRef.Name)

		providerClient := pickedProviderModel.ProviderRef.Client

		comp, err := providerClient.Chat.Completions.New(r.Context(), openai.ChatCompletionNewParams{
			Model:    pickedProviderModel.Id,
			Messages: openaiRequest.Messages,
		})

		if err != nil {
			logger.Errorw("Provider request failed", "error", err, "provider", pickedProviderModel.ProviderRef.Name)
			http.Error(w, "Provider error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(comp); err != nil {
			logger.Errorw("Failed to encode response", "error", err)
			// Примечание: если заголовки уже отправлены, http.Error тут не сработает корректно,
			// но лог мы запишем.
		}
	}
}
