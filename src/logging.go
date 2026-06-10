package api

import (
	"net/http"
	"time"

	"go.uber.org/zap"
)

type LoggingTransport struct {
	BaseTransport http.RoundTripper // обычно http.DefaultTransport
	Logger        *zap.SugaredLogger
}

func (loggingTransport *LoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()

	// Выполняем реальный запрос
	resp, err := loggingTransport.BaseTransport.RoundTrip(req)

	duration := time.Since(start)

	// Логируем результат
	if err != nil {
		loggingTransport.Logger.Errorw("HTTP request failed",
			zap.String("method", req.Method),
			zap.String("url", req.URL.String()),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		loggingTransport.Logger.Infow("HTTP request",
			zap.String("method", req.Method),
			zap.String("url", req.URL.String()),
			zap.Int("status", resp.StatusCode),
			zap.Duration("duration", duration),
		)
	}

	return resp, err
}
