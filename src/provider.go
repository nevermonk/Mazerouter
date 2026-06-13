package core

import (
	"context"
	"net/http"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"go.uber.org/zap"
)

// Provider хранит данные о провайдере и метрики использования.
// Поле authHeader хранит указатель на структуру с шаблоном заголовка.
// При отсутствии пользовательского шаблона в конструкторе создаётся
// значение по умолчанию (Authorization: Bearer <apiKey>).

type ProviderConfig struct {
	Endpoint string
	apiKey   string
}

type Provider struct {
	Name         string
	Config       ProviderConfig
	Models       []*Model
	Client       openai.Client
	ModelAliases map[string][]string

	logger *zap.SugaredLogger
}

// ProviderConfig methods

func newProviderConfig(endpoint string, apiKey string) ProviderConfig {
	return ProviderConfig{
		Endpoint: endpoint,
		apiKey:   apiKey,
	}
}

// Provider methods

func NewProvider(name string, endpoint string, apiKey string, modelAliases map[string][]string, customClientHeaders map[string]string, logger *zap.SugaredLogger) *Provider {
	clientOptions := []option.RequestOption{
		option.WithBaseURL(endpoint),
		option.WithAPIKey(apiKey),
		option.WithHTTPClient(&http.Client{
			Transport: &LoggingTransport{
				BaseTransport: http.DefaultTransport,
				Logger:        logger,
			},
		}),
	}

	for headerKey, headerValue := range customClientHeaders {
		clientOptions = append(clientOptions, option.WithHeader(headerKey, headerValue))
	}

	provider := &Provider{
		Name:         name,
		Config:       newProviderConfig(endpoint, apiKey),
		Models:       []*Model{},
		Client:       openai.NewClient(clientOptions...),
		ModelAliases: modelAliases,
		logger:       logger,
	}

	return provider
}

func (provider *Provider) LoadModels() bool {
	ctx := context.Background()
	provider.logger.Infof("Loading model list for provider %s", provider.Name)
	modelPage, err := provider.Client.Models.List(ctx)
	if err != nil {
		provider.logger.Errorf("Failed to retrieve models: %v", err)
		return false
	}

	// Iterate through the pages of models
	for modelPage != nil {
		for _, model := range modelPage.Data {
			var modelAliases []string
			if _, exists := provider.ModelAliases[model.ID]; exists {
				modelAliases = append(modelAliases, provider.ModelAliases[model.ID]...)
			}
			provider.Models = append(provider.Models, NewModel(model, provider, modelAliases))
		}

		// Advance to the next page if more results exist
		modelPage, err = modelPage.GetNextPage()
		if err != nil {
			provider.logger.Fatalf("Failed to get next page: %v", err)
			return false
		}
	}

	provider.logger.Infof("Loaded total %d models from provider %s", len(provider.Models), provider.Name)
	return true
}

// Обычный провайдер без доп логики
type GenericProvider struct {
	Provider
}
