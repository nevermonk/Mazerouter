package handlers

import (
	"encoding/json"
	"io"
	core "mazerouter/src"
	"net/http"

	"github.com/openai/openai-go/v3"
	"go.uber.org/zap"
)

type RouteHint struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

func HandleOpenaiModelsList(pool *core.ProvidersPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		models := pool.GetAllModels().ToOpenaiModelsList()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(models)
	}
}

// HandleOpenaiCompletions — главный хендлер /v1/chat/completions.
func HandleOpenaiCompletions(pool *core.ProvidersPool, logger *zap.SugaredLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			core.WriteError(w, http.StatusBadRequest, "failed to read body")
			return
		}

		var hint RouteHint
		if err := json.Unmarshal(body, &hint); err != nil {
			core.WriteError(w, http.StatusBadRequest, "invalid JSON")
			return
		}

		var params openai.ChatCompletionNewParams
		if err := json.Unmarshal(body, &params); err != nil {
			core.WriteError(w, http.StatusBadRequest, "invalid params: "+err.Error())
			return
		}

		core.ServeCompletionRequest(
			pool, w, r, hint.Model, hint.Stream, params, logger,
		)
	}
}
