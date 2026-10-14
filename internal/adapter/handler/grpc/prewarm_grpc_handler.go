package grpc

import (
	"io"
	"time"

	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/service"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PreWarmGRPCHandler struct {
	loanv1.UnimplementedLoanPreWarmServiceServer
	prewarmService service.PrewarmService
}

func NewPreWarmGRPCHandler(svc service.PrewarmService) *PreWarmGRPCHandler {
	return &PreWarmGRPCHandler{
		prewarmService: svc,
	}
}

// StreamPreWarmDebitur menerima aliran chunk debitur via gRPC Client Streaming
func (h *PreWarmGRPCHandler) StreamPreWarmDebitur(stream loanv1.LoanPreWarmService_StreamPreWarmDebiturServer) error {
	ctx := stream.Context()
	startTime := time.Now()

	var totalReceived, totalCached, totalFailed int64
	var allFailedNIKs []string
	var activeBatchID string
	chunkCount := 0

	log.Info().
		Str("source", "handler.grpc.StreamPreWarmDebitur").
		Msg("Client streaming connection opened for Pre-Warm Debitur")

	for {
		// 1. Cek Context Cancellation (Graceful Shutdown / Client Timeout)
		if err := ctx.Err(); err != nil {
			log.Warn().
				Err(err).
				Str("source", "handler.grpc.StreamPreWarmDebitur").
				Msg("Stream context cancelled by client or timeout")
			return status.Errorf(codes.Canceled, "client cancelled stream: %v", err)
		}

		// 2. Terima chunk dari client (misal: 500 debitur per pesan)
		chunk, err := stream.Recv()
		if err == io.EOF {
			// Selesai membaca stream dari client, kirim balik ringkasan komprehensif
			durationMs := time.Since(startTime).Milliseconds()
			log.Info().
				Str("batch_id", activeBatchID).
				Int64("total_received", totalReceived).
				Int64("total_cached", totalCached).
				Int64("total_failed", totalFailed).
				Int64("duration_ms", durationMs).
				Int("total_chunks", chunkCount).
				Str("source", "handler.grpc.StreamPreWarmDebitur").
				Msg("Stream completed successfully")

			return stream.SendAndClose(&loanv1.PreWarmSummaryResponse{
				BatchId:       activeBatchID,
				TotalReceived: totalReceived,
				TotalCached:   totalCached,
				TotalFailed:   totalFailed,
				DurationMs:    durationMs,
				FailedNiks:    allFailedNIKs,
			})
		}
		if err != nil {
			log.Error().
				Err(err).
				Str("source", "handler.grpc.StreamPreWarmDebitur").
				Msg("Error receiving chunk from stream")
			return status.Errorf(codes.Internal, "failed to receive chunk: %v", err)
		}

		chunkCount++
		activeBatchID = chunk.GetBatchId()
		debiturs := chunk.GetData()
		totalReceived += int64(len(debiturs))

		// 3. Proses chunk dan simpan ke Redis via Pipeline
		cached, failed, failedNIKs, processErr := h.prewarmService.ProcessChunk(ctx, activeBatchID, debiturs)
		totalCached += cached
		totalFailed += failed
		if len(failedNIKs) > 0 {
			allFailedNIKs = append(allFailedNIKs, failedNIKs...)
		}

		if processErr != nil {
			log.Error().
				Err(processErr).
				Str("batch_id", activeBatchID).
				Int("chunk_index", chunkCount).
				Str("source", "handler.grpc.StreamPreWarmDebitur").
				Msg("Failed to process chunk in redis pipeline")
		}
	}
}
