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

// Результат запроса для non stream запросов
type CompletionResult struct {
	Data     []byte
	Response openai.ChatCompletion
	OK       bool
	Err      ServingError
}

// Результат запроса для stream
type StreamResult struct {
	Chunks chan []byte
	// ReadyForProcessing receives exactly one value once the producer has decided
	// whether the stream is healthy (first chunk read succeeded) or
	// dead (first chunk read failed). The consumer must receive from
	// ReadyForProcessing before reading OK/Err.
	ReadyForProcessing chan struct{}
	OK                 *atomic.Bool
	Err                *atomic.Pointer[ServingError]
	Usage              *atomic.Pointer[openai.CompletionUsage]
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

		// Wait for the producer to know whether the stream is alive.
		select {
		case <-result.ReadyForProcessing:
		case <-r.Context().Done():
			return
		}

		// Read OK/Err AFTER receiving from Status. The channel
		// send/recv establishes a happens-before edge into the
		// producer's writes to OK and Err.
		if !result.OK.Load() {
			seVal := ServingError{StatusCode: 0, ErrorReason: ""}
			if p := result.Err.Load(); p != nil {
				seVal = *p
			}
			logger.Errorf("Model %s of provider %s failed with status code %d and reason '%s'",
				pickedModel.Id, pickedModel.ProviderRef.Name, seVal.StatusCode, seVal.ErrorReason)
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
			fmt.Fprintf(w, "data: %s\n\n", chunk)
			if err := rc.Flush(); err != nil {
				logger.Warnw("flush failed, client likely disconnected", "error", err)
				return
			}
		}
		fmt.Fprint(w, "data: [DONE]\n\n")

		if u := result.Usage.Load(); u != nil {
			logger.Infof("Total tokens used for stream request for provider %s is %d", pickedModel.ProviderRef.Name, u.TotalTokens)
		}
		_ = rc.Flush()
		return
	} else { // non stream запрос

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
		return CompletionResult{
			OK:  false,
			Err: classifyError(err),
		}
	}

	data, err := json.Marshal(resp)
	if err != nil {
		logger.Errorw("encode response error", "error", err)
		return CompletionResult{OK: false}
	}

	return CompletionResult{Data: data, Response: *resp, OK: true}
}

func ServeStream(
	ctx context.Context,
	client openai.Client,
	params openai.ChatCompletionNewParams,
	logger *zap.SugaredLogger,
) StreamResult {
	stream := client.Chat.Completions.NewStreaming(ctx, params)
	chunks := make(chan []byte, 64)
	readyForProcessing := make(chan struct{}, 1)

	ok := &atomic.Bool{}
	se := &atomic.Pointer[ServingError]{}
	usage := &atomic.Pointer[openai.CompletionUsage]{}

	go func() {
		defer stream.Close()
		defer close(chunks)

		if !stream.Next() {
			ok.Store(false)
			seVal := classifyError(stream.Err())
			se.Store(&seVal)
			readyForProcessing <- struct{}{}
			return
		}

		ok.Store(true)
		empty := ServingError{}
		se.Store(&empty)
		readyForProcessing <- struct{}{}

		var acc openai.ChatCompletionAccumulator
		acc.AddChunk(stream.Current())
		data, _ := json.Marshal(stream.Current())
		select {
		case chunks <- data:
		case <-ctx.Done():
			return
		}

		for stream.Next() {
			data, _ := json.Marshal(stream.Current())
			select {
			case chunks <- data:
			case <-ctx.Done():
				return
			}
			acc.AddChunk(stream.Current())
		}

		if stream.Err() == nil {
			usage.Store(&acc.Usage)
		}

		if err := stream.Err(); err != nil && !errors.Is(err, context.Canceled) {
			logger.Errorw("stream error mid-flight", "error", err)
			errJSON, _ := json.Marshal(map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "stream_error"},
			})
			select {
			case chunks <- errJSON:
			case <-ctx.Done():
			}
		}
	}()

	return StreamResult{
		Chunks:             chunks,
		ReadyForProcessing: readyForProcessing,
		OK:                 ok,
		Err:                se,
		Usage:              usage,
	}
}

// classifyError turns an error into a ServingError with a status code.
// Used by both stream and non-stream requests.
func classifyError(err error) ServingError {
	if err == nil {
		return ServingError{
			StatusCode:  http.StatusBadGateway,
			ErrorReason: "no error but no response",
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ServingError{StatusCode: 0, ErrorReason: "client cancelled"}
	}
	var apiErr *openai.Error
	if errors.As(err, &apiErr) && apiErr.StatusCode != 0 {
		return ServingError{StatusCode: apiErr.StatusCode, ErrorReason: "Upstream Error"}
	}
	return ServingError{StatusCode: http.StatusBadGateway, ErrorReason: err.Error()}
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
