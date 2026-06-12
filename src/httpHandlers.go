package api

import (
	"context"
	"encoding/json"
	"errors"
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

type servingError struct {
	StatusCode  int
	ErrorReason string
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

		// 2. Полный парс — напрямую в SDK-тип, без встраивания.
		// ChatCompletionNewParams поддерживает UnmarshalJSON через apijson.
		var params openai.ChatCompletionNewParams
		if err := json.Unmarshal(body, &params); err != nil {
			writeError(w, http.StatusBadRequest, "invalid params: "+err.Error())
			return
		}

		serveCompletionRequest(
			pool, w, r, hint, params, logger,
		)
	}
}

func serveCompletionRequest(
	pool *ProvidersPool,
	w http.ResponseWriter,
	r *http.Request,
	hint routeHint,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) {
	pickedModel := pool.GetModelRoute(hint.Model)
	if pickedModel == nil {
		writeError(w, http.StatusNotFound, fmt.Sprintf("no route for model %q", hint.Model))
		return
	}

	logger.Infow("routing",
		"incoming", hint.Model,
		"provider", pickedModel.ProviderRef.Name,
		"upstreamModel", pickedModel.Id,
		"stream", hint.Stream,
	)
	// Подменяем model на upstream ID
	params.Model = openai.ChatModel(pickedModel.Id)

	if hint.Stream {
		if ok, se := serveStream(w, r.Context(), pickedModel.ProviderRef.Client, params, logger); !ok {
			logger.Errorf("Model %s of provider %s failed with status code %s and reason '%s'", pickedModel.Id, pickedModel.ProviderRef.Name, se.StatusCode, se.ErrorReason)
			pickedModel.Awailable = false // показываем, что модель недоступна TBD: проверка доступности
			serveCompletionRequest(
				pool, w, r, hint, params, logger,
			) // рекурсивно перезапускаемся
			return
		}
	} else {
		serveCompletion(w, r.Context(), pickedModel.ProviderRef.Client, params, logger)
	}
}

func serveCompletion(
	w http.ResponseWriter,
	ctx context.Context,
	client openai.Client,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) (bool, servingError) {
	resp, err := client.Chat.Completions.New(ctx, params)
	if err != nil {
		logger.Errorw("upstream error", "error", err)
		writeError(w, http.StatusBadGateway, "upstream error: "+err.Error())
		var apiErr *openai.Error
		errors.As(err, &apiErr)
		return false, servingError{
			StatusCode:  apiErr.StatusCode,
			ErrorReason: "Upstream Error",
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.Errorw("encode response error", "error", err)
	}

	return true, servingError{}
}

func serveStream(
	w http.ResponseWriter,
	ctx context.Context,
	client openai.Client,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) (bool, servingError) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)

	// Проверяем поддержку flush через контроллер (разворачивает middleware-цепочку)
	if err := rc.Flush(); err != nil {
		logger.Errorw("flush not supported", "error", err)
		return false, servingError{
			ErrorReason: "Streaming unsupported for model",
		}
	}

	stream := client.Chat.Completions.NewStreaming(ctx, params)
	defer stream.Close()

	completionsAccumulator := openai.ChatCompletionAccumulator{}

	if !stream.Next() {
		// обарабатываем ошибку при попытке получить первый чанк
		err := stream.Err()

		var apiErr *openai.Error
		errors.As(err, &apiErr)

		if apiErr.StatusCode == 429 {
			return false, servingError{
				StatusCode:  429,
				ErrorReason: "Too many requests for model",
			}
		}
		stream.Close()
	} else {
		firstChunk := stream.Current()
		sendStreamChunk(w, rc, firstChunk, &completionsAccumulator, logger)
		for stream.Next() {
			chunk := stream.Current()
			sendStreamChunk(w, rc, chunk, &completionsAccumulator, logger)
		}
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

	return true, servingError{}
}

func sendStreamChunk(
	w http.ResponseWriter,
	rc *http.ResponseController,
	chunk openai.ChatCompletionChunk,
	acc *openai.ChatCompletionAccumulator,
	logger *zap.SugaredLogger,
) {
	if !acc.AddChunk(chunk) {
		logger.Errorw("failed to accumulate chunk")
	}

	// Check if a tool call just finished - useful for logging
	if finished, ok := acc.JustFinishedToolCall(); ok {
		logger.Infow("tool call completed",
			"name", finished.Name,
			"args", finished.Arguments,
		)
	}

	data, err := json.Marshal(chunk)
	if err != nil {
		logger.Errorw("marshal chunk error", "error", err)
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", data)
	rc.Flush()
}

// hasAccumulatedContent checks if accumulated response has meaningful content
func hasAccumulatedContent(cc openai.ChatCompletion) bool {
	if len(cc.Choices) == 0 {
		return false
	}
	msg := cc.Choices[0].Message
	return msg.Content != "" ||
		msg.Role != "" ||
		len(msg.ToolCalls) > 0
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
