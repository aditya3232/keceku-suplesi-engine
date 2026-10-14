package service

import (
	"context"
	"fmt"
	"runtime"
	"time"

	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/domain/model"
	"github.com/rs/zerolog/log"
)

type FeederService interface {
	RunFeeder(ctx context.Context, cfg model.FeederJobConfig, client loanv1.LoanPreWarmServiceClient) (*model.FeederJobResult, error)
}

type feederService struct{}

func NewFeederService() FeederService {
	return &feederService{}
}

func (f *feederService) RunFeeder(ctx context.Context, cfg model.FeederJobConfig, client loanv1.LoanPreWarmServiceClient) (*model.FeederJobResult, error) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = 500
	}
	if cfg.TotalCount <= 0 {
		cfg.TotalCount = 100000
	}
	if cfg.BatchID == "" {
		cfg.BatchID = fmt.Sprintf("BATCH-%d", time.Now().Unix())
	}

	log.Info().
		Str("batch_id", cfg.BatchID).
		Int("total_count", cfg.TotalCount).
		Int("chunk_size", cfg.ChunkSize).
		Str("source", "service.feederService.RunFeeder").
		Msg("Starting Batch Feeder Client Streaming to Loan Service")

	// 1. Inisialisasi gRPC Client Stream
	stream, err := client.StreamPreWarmDebitur(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open gRPC client stream: %w", err)
	}

	// 2. Channel data generator dengan bounded buffer untuk menjaga heap konstan (< 25 MB)
	dataChan := make(chan *loanv1.DebiturCandidate, cfg.ChunkSize*2)
	go generateSyntheticDebiturStream(cfg.TotalCount, dataChan)

	startTime := time.Now()
	chunk := make([]*loanv1.DebiturCandidate, 0, cfg.ChunkSize)
	chunkIndex := 0
	totalDispatched := 0

	for debitur := range dataChan {
		chunk = append(chunk, debitur)
		totalDispatched++

		if len(chunk) >= cfg.ChunkSize {
			chunkIndex++
			sendErr := stream.Send(&loanv1.PreWarmDebiturChunk{
				BatchId: cfg.BatchID,
				Data:    chunk,
			})
			if sendErr != nil {
				return nil, fmt.Errorf("failed sending chunk %d: %w", chunkIndex, sendErr)
			}

			if chunkIndex%20 == 0 || totalDispatched == cfg.TotalCount {
				log.Info().
					Str("batch_id", cfg.BatchID).
					Int("chunk", chunkIndex).
					Int("dispatched", totalDispatched).
					Msg("Chunk streaming progress")
			}

			// Reset slice buffer
			chunk = make([]*loanv1.DebiturCandidate, 0, cfg.ChunkSize)
		}
	}

	// 3. Kirim sisa data terakhir jika ada
	if len(chunk) > 0 {
		chunkIndex++
		sendErr := stream.Send(&loanv1.PreWarmDebiturChunk{
			BatchId: cfg.BatchID,
			Data:    chunk,
		})
		if sendErr != nil {
			return nil, fmt.Errorf("failed sending final chunk: %w", sendErr)
		}
	}

	// 4. Tutup stream dan terima ringkasan respons dari server
	summary, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("error waiting stream summary from server: %w", err)
	}

	duration := time.Since(startTime)
	durationSec := duration.Seconds()
	if durationSec <= 0 {
		durationSec = 0.001
	}
	throughput := float64(totalDispatched) / durationSec

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	heapAllocMB := float64(memStats.Alloc) / (1024 * 1024)

	result := &model.FeederJobResult{
		BatchID:         cfg.BatchID,
		TotalDispatched: totalDispatched,
		TotalCached:     summary.GetTotalCached(),
		TotalFailed:     summary.GetTotalFailed(),
		DurationMs:      duration.Milliseconds(),
		ThroughputRPS:   throughput,
		MemoryAllocMB:   heapAllocMB,
	}

	log.Info().
		Str("batch_id", cfg.BatchID).
		Int("total_dispatched", result.TotalDispatched).
		Int64("total_cached", result.TotalCached).
		Int64("total_failed", result.TotalFailed).
		Int64("duration_ms", result.DurationMs).
		Float64("throughput_rps", result.ThroughputRPS).
		Float64("memory_alloc_mb", result.MemoryAllocMB).
		Msg("Batch Feeder completed successfully")

	return result, nil
}

// generateSyntheticDebiturStream mensimulasikan pembacaan baris data dari Data Lake / DB Replika secara kursor
func generateSyntheticDebiturStream(total int, out chan<- *loanv1.DebiturCandidate) {
	defer close(out)

	tenors := []int32{12, 24, 36}
	for i := 1; i <= total; i++ {
		nik := fmt.Sprintf("320101%010d", i)
		rekening := fmt.Sprintf("012301%09d", i)
		telepon := fmt.Sprintf("62812%08d", i)

		// Angka realistis Kupedes BRI: Plafond 50 jt, Baki debet 20 jt, Tawaran Suplesi 30 jt
		bakiDebet := int64(20_000_000)
		originalPlafond := int64(50_000_000)
		maxSuplesi := int64(30_000_000)

		out <- &loanv1.DebiturCandidate{
			Nik:               nik,
			Nama:              fmt.Sprintf("Debitur Kupedes %d", i),
			NomorRekening:     rekening,
			NomorTelepon:      telepon,
			UnitKerjaId:       "UNIT-BRI-0421",
			ExistingBakiDebet: bakiDebet,
			OriginalPlafond:   originalPlafond,
			MaxSuplesiAmount:  maxSuplesi,
			AllowedTenors:     tenors,
		}
	}
}
