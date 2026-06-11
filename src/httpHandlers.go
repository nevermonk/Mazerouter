package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/shared"
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

type OpenaiIncomingChatRequest struct {
	openai.ChatCompletionNewParams      // Встраиваем все поля SDK
	Stream                         bool `json:"stream"` // Добавляем наше поле для стриминга
}

func HandleOpenaiCompletions(pool *ProvidersPool, logger *zap.SugaredLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logger.Errorf("Failed to read request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var openaiRequest OpenaiIncomingChatRequest
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

		// 3. Готовим параметры для провайдера (встроенный ChatCompletionNewParams)
		providerParams := openaiRequest.ChatCompletionNewParams
		providerParams.Model = shared.ChatModel(pickedProviderModel.Id)

		providerClient := pickedProviderModel.ProviderRef.Client

		if openaiRequest.Stream {
			handleStreaming(w, r, providerClient, providerParams, logger)
			return
		}

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

func handleStreaming(w http.ResponseWriter, r *http.Request, client openai.Client, params openai.ChatCompletionNewParams, logger *zap.SugaredLogger) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	stream := client.Chat.Completions.NewStreaming(r.Context(), params)
	defer stream.Close()

	for stream.Next() {
		chunk := stream.Current()
		chunkJSON, err := json.Marshal(chunk)
		if err != nil {
			logger.Errorw("Failed to marshal chunk", "error", err)
			continue
		}
		fmt.Fprintf(w, "data: %s\n\n", chunkJSON)
		flusher.Flush()
	}

	if err := stream.Err(); err != nil {
		if err == context.Canceled {
			logger.Infow("Client disconnected")
		} else {
			logger.Errorw("Stream error", "error", err)
		}
	} else {
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}
}
