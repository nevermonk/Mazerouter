package api

import (
	"context"
	"encoding/json"
	"fmt"
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

type routeHint struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

// HandleOpenaiCompletions — главный хендлер /v1/chat/completions.
func HandleOpenaiCompletions(pool *ProvidersPool, logger *zap.SugaredLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "failed to read body")
			return
		}

		// 1. Быстрый парс только для выбора провайдера
		var hint routeHint
		if err := json.Unmarshal(body, &hint); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON")
			return
		}

		route := pool.GetModelRoute(hint.Model)
		if route == nil {
			writeError(w, http.StatusNotFound, fmt.Sprintf("no route for model %q", hint.Model))
			return
		}

		logger.Infow("routing",
			"incoming", hint.Model,
			"provider", route.ProviderRef.Name,
			"upstreamModel", route.Id,
			"stream", hint.Stream,
		)

		// 2. Полный парс — напрямую в SDK-тип, без встраивания.
		// ChatCompletionNewParams поддерживает UnmarshalJSON через apijson.
		var params openai.ChatCompletionNewParams
		if err := json.Unmarshal(body, &params); err != nil {
			writeError(w, http.StatusBadRequest, "invalid params: "+err.Error())
			return
		}
		// Подменяем model на upstream ID
		params.Model = openai.ChatModel(route.Id)

		if hint.Stream {
			serveStream(w, r.Context(), route.ProviderRef.Client, params, logger)
		} else {
			serveCompletion(w, r.Context(), route.ProviderRef.Client, params, logger)
		}
	}
}

func serveCompletion(
	w http.ResponseWriter,
	ctx context.Context,
	client openai.Client,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) {
	resp, err := client.Chat.Completions.New(ctx, params)
	if err != nil {
		logger.Errorw("upstream error", "error", err)
		writeError(w, http.StatusBadGateway, "upstream error: "+err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Errorw("encode response error", "error", err)
	}
}

func serveStream(
	w http.ResponseWriter,
	ctx context.Context,
	client openai.Client,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)

	// Проверяем поддержку flush через контроллер (разворачивает middleware-цепочку)
	if err := rc.Flush(); err != nil {
		logger.Errorw("flush not supported", "error", err)
		return
	}

	stream := client.Chat.Completions.NewStreaming(ctx, params)
	defer stream.Close()

	for stream.Next() {
		chunk := stream.Current()

		// 🔍 ОТЛАДКА: Логируем сырой контент аргументов
		if len(chunk.Choices) > 0 && len(chunk.Choices[0].Delta.ToolCalls) > 0 {
			for _, tc := range chunk.Choices[0].Delta.ToolCalls {
				if tc.Function.Arguments != "" {
					logger.Infow("raw arguments chunk",
						"name", tc.Function.Name,
						"raw_bytes", string(tc.Function.Arguments), // Покажет сырые байты
					)
				}
			}
		}
		data, err := json.Marshal(chunk)
		if err != nil {
			logger.Errorw("marshal chunk error", "error", err)
			continue
		}
		fmt.Fprintf(w, "data: %s\n\n", data)
		rc.Flush() // игнорируем ошибку в hot-path — клиент мог отключиться
	}

	switch err := stream.Err(); {
	case err == nil:
		fmt.Fprint(w, "data: [DONE]\n\n")
		rc.Flush()
	case err == context.Canceled:
		logger.Infow("client disconnected")
	default:
		logger.Errorw("stream error", "error", err)
		errJSON, _ := json.Marshal(map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "stream_error"},
		})
		fmt.Fprintf(w, "data: %s\n\n", errJSON)
		rc.Flush()
	}
}

// writeError — OpenAI-совместимый JSON-ответ с ошибкой.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"message": msg,
			"type":    "proxy_error",
		},
	})
}
