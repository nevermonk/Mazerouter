package api

import (
	"encoding/json"
	"net/http"
)

func HandleModelsList(pool *ProvidersPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		models := pool.GetAllModels()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(models)
	}
}
