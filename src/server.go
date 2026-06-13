package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/openai/openai-go/v3"
	"go.uber.org/zap"
)

type servingError struct {
	StatusCode  int
	ErrorReason string
}

func ServeCompletionRequest(
	pool *ProvidersPool,
	w http.ResponseWriter,
	r *http.Request,
	hint RouteHint,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) {
	pickedModel := pool.GetModelRoute(hint.Model)
	if pickedModel == nil {
		WriteError(w, http.StatusNotFound, fmt.Sprintf("no route for model %q", hint.Model))
		return
	}

	logger.Infow("routing",
		"incoming", hint.Model,
		"provider", pickedModel.ProviderRef.Name,
		"upstreamModel", pickedModel.Id,
		"stream", hint.Stream,
	)
	params.Model = openai.ChatModel(pickedModel.Id)

	if hint.Stream {
		if ok, se := serveStream(w, r.Context(), pickedModel.ProviderRef.Client, params, logger); !ok {
			logger.Errorf("Model %s of provider %s failed with status code %s and reason '%s'", pickedModel.Id, pickedModel.ProviderRef.Name, se.StatusCode, se.ErrorReason)
			pickedModel.Awailable = false
			ServeCompletionRequest(
				pool, w, r, hint, params, logger,
			)
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
		WriteError(w, http.StatusBadGateway, "upstream error: "+err.Error())
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

func hasAccumulatedContent(cc openai.ChatCompletion) bool {
	if len(cc.Choices) == 0 {
		return false
	}
	msg := cc.Choices[0].Message
	return msg.Content != "" ||
		msg.Role != "" ||
		len(msg.ToolCalls) > 0
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
