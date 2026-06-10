package main

import (
	"encoding/json"
	"flag"
	"net/http"
	"os"

	api "megarouter/src"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type ProviderYAML struct {
	Name     string `yaml:"name"`
	Endpoint string `yaml:"endpoint"`
	APIKey   string `yaml:"apiKey"`
}

type Config struct {
	Providers []ProviderYAML `yaml:"providers"`
}

func main() {
	loggerCfg := zap.NewProductionConfig()
	loggerObj, err := loggerCfg.Build()
	if err != nil {
		panic(err)
	}

	defer loggerObj.Sync()

	logger := loggerObj.Sugar()

	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	logger.Info("Welcome to Maze...")

	data, err := os.ReadFile(*configPath)
	if err != nil {
		logger.Errorf("Failed to read config file: %v", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		logger.Fatalf("Failed to parse config file: %v", err)
	}

	providerPool := api.ProvidersPool{}
	for _, p := range config.Providers {
		provider := api.NewProvider(p.Name, p.Endpoint, p.APIKey, logger)
		provider.LoadModels()

		providerPool.Providers = append(providerPool.Providers, provider)
	}

	logger.Info("Providers initial loading complete")

	r := chi.NewRouter()
	r.Get("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		models := providerPool.GetAllModels()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(models)
	})

	logger.Info("Server starting on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		logger.Fatalf("Server failed: %v", err)
	}
}
