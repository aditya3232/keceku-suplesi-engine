package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
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
	app.Get("/api/v1/token/:nik", handler.GetTokenByNIK)
	app.Get("/api/v1/stats", handler.GetStats)
	app.Post("/api/v1/prewarm", handler.PrewarmBatch)

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

func (h *OfferHandler) GetTokenByNIK(c fiber.Ctx) error {
	nik := c.Params("nik")
	if nik == "" {
		return c.Status(http.StatusBadRequest).JSON(response.DefaultResponse{
			Message: "NIK tidak boleh kosong",
			Data:    nil,
		})
	}

	ctx := c.Context()
	token, err := h.prewarmService.GetTokenByNIK(ctx, nik)
	if err != nil {
		return c.Status(http.StatusNotFound).JSON(response.DefaultResponse{
			Message: "Token untuk NIK ini belum tersedia atau sudah kedaluwarsa",
			Data:    nil,
		})
	}

	return c.Status(http.StatusOK).JSON(response.DefaultResponse{
		Message: "success",
		Data: fiber.Map{
			"nik":   nik,
			"token": token,
			"url":   fmt.Sprintf("http://localhost:8090/s/%s", token),
		},
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

type PrewarmBatchRequest struct {
	BatchID string                    `json:"batch_id"`
	Data    []*loanv1.DebiturCandidate `json:"data"`
}

// PrewarmBatch memfasilitasi pengujian pre-warm melalui HTTP REST (berguna untuk cURL / Bruno)
func (h *OfferHandler) PrewarmBatch(c fiber.Ctx) error {
	var req PrewarmBatchRequest
	if err := json.Unmarshal(c.Body(), &req); err != nil {
		return c.Status(http.StatusBadRequest).JSON(response.DefaultResponse{
			Message: "format request body tidak valid: " + err.Error(),
			Data:    nil,
		})
	}

	if len(req.Data) == 0 {
		return c.Status(http.StatusBadRequest).JSON(response.DefaultResponse{
			Message: "data debitur tidak boleh kosong",
			Data:    nil,
		})
	}

	if req.BatchID == "" {
		req.BatchID = fmt.Sprintf("REST-%d", time.Now().Unix())
	}

	ctx := c.Context()
	cached, failed, failedNIKs, err := h.prewarmService.ProcessChunk(ctx, req.BatchID, req.Data)
	if err != nil {
		return c.Status(http.StatusInternalServerError).JSON(response.DefaultResponse{
			Message: "gagal memproses chunk debitur: " + err.Error(),
			Data:    nil,
		})
	}

	return c.Status(http.StatusOK).JSON(response.DefaultResponse{
		Message: "success",
		Data: fiber.Map{
			"batch_id":       req.BatchID,
			"total_received": len(req.Data),
			"total_cached":   cached,
			"total_failed":   failed,
			"failed_niks":    failedNIKs,
		},
	})
}
