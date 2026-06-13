package main

import (
	"flag"
	"net/http"
	"os"
	"time"

	core "mazerouter/src"
	handlers "mazerouter/src/handlers"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

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

	providerPool := core.ProvidersPool{PickStrategy: config.Settings.Providers.PickStrategy}
	logger.Infof("Provider pick strategy - %s", config.Settings.Providers.PickStrategy)
	for _, p := range config.Providers {
		provider := core.NewProvider(p.Name, p.Endpoint, p.APIKey, p.Settings.ModelAliases, p.Settings.Http.CustomHeaders, logger)
		go initProvider(&providerPool, provider)
	}

	logger.Info("Providers initial loading complete")

	startServing(&providerPool, logger)
}

func initProvider(pool *core.ProvidersPool, provider *core.Provider) {
	loadModelsResult := provider.LoadModels()
	if loadModelsResult {
		pool.Providers = append(pool.Providers, provider)
	}
}

func startServing(providerPool *core.ProvidersPool, logger *zap.SugaredLogger) {
	r := chi.NewRouter()
	r.Use(NewChiZapLoggerMiddleware(logger))

	r.Get("/v1/models", handlers.MazeModelsList(providerPool))
	r.Get("/openai/v1/models", handlers.HandleOpenaiModelsList(providerPool))
	r.Post("/openai/v1/chat/completions", handlers.HandleOpenaiCompletions(providerPool, logger))

	logger.Info("Server starting on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		logger.Fatalf("Server failed: %v", err)
	}
}

func NewChiZapLoggerMiddleware(logger *zap.SugaredLogger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := &responseWriter{ResponseWriter: w, status: 200}
			next.ServeHTTP(ww, r)
			duration := time.Since(start)
			logger.Infow("HTTP request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", ww.status),
				zap.Duration("duration", duration),
			)
		})
	}
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

type Config struct {
	Providers []Provider `yaml:"providers"`
	Settings  Settings   `yaml:"settings"`
}
type Provider struct {
	Name     string           `yaml:"name"`
	Endpoint string           `yaml:"endpoint"`
	APIKey   string           `yaml:"apiKey"`
	Settings ProviderSettings `yaml:"settings"`
}

type ProviderSettings struct {
	ModelAliases map[string][]string  `yaml:"modelAliases"`
	Http         ProviderSettingsHttp `yaml:"http"`
}

type ProviderSettingsHttp struct {
	CustomHeaders map[string]string `yaml:"customHeaders"`
}

type Settings struct {
	Providers SettingsProviders `yaml:"providers"`
}

type SettingsProviders struct {
	PickStrategy string `yaml:"pickStrategy"`
}
