package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/openai/openai-go/v3"
	"go.uber.org/zap"
)

type ServingError struct {
	StatusCode  int
	ErrorReason string
}

type CompletionResult struct {
	Data []byte
	OK   bool
	Err  ServingError
}

type StreamResult struct {
	Chunks chan []byte
	OK     bool
	Err    ServingError
}

func ServeCompletionRequest(
	pool *ProvidersPool,
	w http.ResponseWriter,
	r *http.Request,
	model string,
	stream bool,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) {
	pickedModel := pool.GetModelRoute(model)
	if pickedModel == nil {
		WriteError(w, http.StatusNotFound, fmt.Sprintf("no route for model %q", model))
		return
	}

	logger.Infow("routing",
		"incoming", model,
		"provider", pickedModel.ProviderRef.Name,
		"upstreamModel", pickedModel.Id,
		"stream", stream,
	)
	params.Model = openai.ChatModel(pickedModel.Id)

	if stream {
		result := ServeStream(r.Context(), pickedModel.ProviderRef.Client, params, logger)
		if !result.OK {
			logger.Errorf("Model %s of provider %s failed with status code %s and reason '%s'", pickedModel.Id, pickedModel.ProviderRef.Name, result.Err.StatusCode, result.Err.ErrorReason)
			pickedModel.Awailable = false
			ServeCompletionRequest(pool, w, r, model, stream, params, logger)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(http.StatusOK)

		rc := http.NewResponseController(w)
		for chunk := range result.Chunks {
			logger.Infow("Writing sse chunk back to user...")
			fmt.Fprintf(w, "data: %s\n\n", chunk)
			rc.Flush()
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		rc.Flush()
	} else {
		result := ServeCompletion(r.Context(), pickedModel.ProviderRef.Client, params, logger)
		if !result.OK {
			WriteError(w, http.StatusBadGateway, "upstream error: "+result.Err.ErrorReason)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(result.Data)
	}
}

func ServeCompletion(
	ctx context.Context,
	client openai.Client,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) CompletionResult {
	resp, err := client.Chat.Completions.New(ctx, params)
	if err != nil {
		logger.Errorw("upstream error", "error", err)
		var apiErr *openai.Error
		errors.As(err, &apiErr)
		return CompletionResult{
			OK: false,
			Err: ServingError{
				StatusCode:  apiErr.StatusCode,
				ErrorReason: "Upstream Error",
			},
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		logger.Errorw("encode response error", "error", err)
		return CompletionResult{OK: false}
	}

	return CompletionResult{Data: data, OK: true}
}

func ServeStream(
	ctx context.Context,
	client openai.Client,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) StreamResult {
	stream := client.Chat.Completions.NewStreaming(ctx, params)

	chunks := make(chan []byte)
	var ok atomic.Bool
	var se ServingError

	go func() {
		defer stream.Close()
		defer close(chunks)

		if !stream.Next() {
			err := stream.Err()
			var apiErr *openai.Error
			if errors.As(err, &apiErr) && apiErr.StatusCode == 429 {
				ok.Store(false)
				se = ServingError{
					StatusCode:  429,
					ErrorReason: "Too many requests for model",
				}
			}
			return
		}

		ok.Store(true)
		firstChunk := stream.Current()
		data, _ := json.Marshal(firstChunk)
		chunks <- data

		for stream.Next() {
			chunk := stream.Current()
			data, _ := json.Marshal(chunk)
			chunks <- data
		}

		if err := stream.Err(); err != nil && !errors.Is(err, context.Canceled) {
			logger.Errorw("stream error", "error", err)
			errJSON, _ := json.Marshal(map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "stream_error"},
			})
			chunks <- errJSON
		}
	}()

	<-ctx.Done()

	return StreamResult{Chunks: chunks, OK: ok.Load(), Err: se}
}

func WriteError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"message": msg,
			"type":    "proxy_error",
		},
	})
}
