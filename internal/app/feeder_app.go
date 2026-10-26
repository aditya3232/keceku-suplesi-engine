package app

import (
	"context"
	"fmt"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/config"
	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/domain/model"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/service"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RunFeeder menginisialisasi konfigurasi, koneksi gRPC client, dan menjalankan FeederService.
// Fungsi ini menjadi entry point yang dipanggil oleh CLI command (cmd/feeder.go) maupun pilot test
// untuk mengalirkan batch kandidat debitur ke server serta mencetak ringkasan metrik eksekusi ke konsol.
func RunFeeder(batchID string, totalCount int, chunkSize int, target string) (*model.FeederJobResult, error) {
	cfg := config.NewConfig()
	config.NewLogger(cfg.App.AppEnv, cfg.App.LogLevel, "batch-feeder")

	if target == "" {
		target = cfg.Feeder.ServerGrpcTarget
	}
	if totalCount <= 0 {
		totalCount = cfg.Feeder.TotalCandidates
	}
	if chunkSize <= 0 {
		chunkSize = cfg.Feeder.BatchSize
	}
	if batchID == "" {
		batchID = fmt.Sprintf("BATCH-%d", time.Now().Unix())
	}

	log.Info().
		Str("target", target).
		Int("total_count", totalCount).
		Int("chunk_size", chunkSize).
		Str("batch_id", batchID).
		Msg("Connecting to gRPC Loan Service for Batch Feeding")

	// Koneksi gRPC client
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error().
			Err(err).
			Str("target", target).
			Msg("Failed to connect to gRPC server")
		return nil, err
	}
	defer conn.Close()

	client := loanv1.NewLoanPreWarmServiceClient(conn)
	feederService := service.NewFeederService()

	jobCfg := model.FeederJobConfig{
		BatchID:          batchID,
		TotalCount:       totalCount,
		ChunkSize:        chunkSize,
		ServerGrpcTarget: target,
	}

	ctx := context.Background()
	result, err := feederService.RunFeeder(ctx, jobCfg, client)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Batch feeder execution failed")
		return nil, err
	}

	fmt.Println("\n==================================================================")
	fmt.Println("🚀 BATCH PRE-WARM DISPATCH SUMMARY")
	fmt.Println("==================================================================")
	fmt.Printf("Batch ID          : %s\n", result.BatchID)
	fmt.Printf("Total Dispatched  : %d debiturs\n", result.TotalDispatched)
	fmt.Printf("Total Cached      : %d debiturs\n", result.TotalCached)
	fmt.Printf("Total Failed      : %d debiturs\n", result.TotalFailed)
	fmt.Printf("Duration          : %d ms (%.2f s)\n", result.DurationMs, float64(result.DurationMs)/1000.0)
	fmt.Printf("Throughput        : %.2f records/second\n", result.ThroughputRPS)
	fmt.Printf("Heap Memory Used  : %.2f MB\n", result.MemoryAllocMB)
	fmt.Println("==================================================================")

	return result, nil
}
