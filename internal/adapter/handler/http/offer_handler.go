package http

import (
	"net/http"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/internal/adapter/handler/response"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/service"
	"github.com/gofiber/fiber/v3"
	"github.com/rs/zerolog/log"
)

type OfferHandler struct {
	prewarmService service.PrewarmService
}

func NewOfferHandler(app *fiber.App, svc service.PrewarmService) *OfferHandler {
	handler := &OfferHandler{
		prewarmService: svc,
	}

	// URL pendek simulasi klik WhatsApp: /s/:token
	app.Get("/s/:token", handler.GetOffering)
	app.Get("/api/v1/offer/:token", handler.GetOffering)
	app.Get("/api/v1/stats", handler.GetStats)

	return handler
}

func (h *OfferHandler) GetOffering(c fiber.Ctx) error {
	token := c.Params("token")
	if token == "" {
		return c.Status(http.StatusBadRequest).JSON(response.DefaultResponse{
			Message: "token tidak boleh kosong",
			Data:    nil,
		})
	}

	start := time.Now()
	ctx := c.Context()

	offer, err := h.prewarmService.GetOfferingByToken(ctx, token)
	if err != nil {
		log.Warn().
			Str("token", token).
			Str("source", "handler.http.GetOffering").
			Msg("Offer not found or expired")

		return c.Status(http.StatusNotFound).JSON(response.DefaultResponse{
			Message: "Penawaran tidak ditemukan atau masa berlaku 48 jam telah berakhir",
			Data:    nil,
		})
	}

	latency := time.Since(start)
	log.Debug().
		Str("token", token).
		Str("latency", latency.String()).
		Msg("Offer retrieved from Redis")

	return c.Status(http.StatusOK).JSON(response.DefaultResponse{
		Message: "success",
		Data:    offer,
	})
}

func (h *OfferHandler) GetStats(c fiber.Ctx) error {
	ctx := c.Context()
	totalCached, err := h.prewarmService.GetTotalCached(ctx)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(response.DefaultResponse{
			Message: err.Error(),
			Data:    nil,
		})
	}

	return c.Status(http.StatusOK).JSON(response.DefaultResponse{
		Message: "success",
		Data: fiber.Map{
			"total_cached_keys": totalCached,
		},
	})
}
