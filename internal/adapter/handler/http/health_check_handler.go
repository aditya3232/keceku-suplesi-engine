package http

import (
	"net/http"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/internal/adapter/handler/response"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/domain/model"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/service"
	"github.com/gofiber/fiber/v3"
	"github.com/rs/zerolog/log"
)

type HealthCheckHandler struct {
	prewarmService service.PrewarmService
}

func NewHealthCheckHandler(app *fiber.App, svc service.PrewarmService) *HealthCheckHandler {
	handler := &HealthCheckHandler{
		prewarmService: svc,
	}

	app.Get("/health", handler.HealthCheck)
	app.Get("/api/check", handler.HealthCheck)

	return handler
}

func (h *HealthCheckHandler) HealthCheck(c fiber.Ctx) error {
	ctx := c.Context()

	redisStatus := "UP"
	if err := h.prewarmService.CheckHealth(ctx); err != nil {
		redisStatus = "DOWN"
		log.Error().
			Err(err).
			Str("source", "handler.http.HealthCheck").
			Msg("Redis health check failed")

		return c.Status(http.StatusServiceUnavailable).JSON(response.DefaultResponse{
			Message: "service unhealthy",
			Data: model.HealthStatus{
				Status:    "DEGRADED",
				Redis:     redisStatus,
				Timestamp: time.Now().Format(time.RFC3339),
			},
		})
	}

	return c.Status(http.StatusOK).JSON(response.DefaultResponse{
		Message: "service healthy",
		Data: model.HealthStatus{
			Status:    "UP",
			Redis:     redisStatus,
			Timestamp: time.Now().Format(time.RFC3339),
		},
	})
}
