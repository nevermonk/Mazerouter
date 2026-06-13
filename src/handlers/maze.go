package handlers

import (
	"encoding/json"
	core "mazerouter/src"
	"net/http"
)

func MazeModelsList(pool *core.ProvidersPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		models := pool.GetAllModels().ToMazeModelsList()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(models)
	}
}
