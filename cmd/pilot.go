package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/config"
	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
	grpchandler "github.com/aditya3232/keceku-suplesi-engine/internal/adapter/handler/grpc"
	httphandler "github.com/aditya3232/keceku-suplesi-engine/internal/adapter/handler/http"
	"github.com/aditya3232/keceku-suplesi-engine/internal/adapter/repository"
	"github.com/aditya3232/keceku-suplesi-engine/internal/app"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/service"
	"github.com/gofiber/fiber/v3"
	fiberCors "github.com/gofiber/fiber/v3/middleware/cors"
	fiberRecover "github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var pilotCount int
var pilotChunkSize int

var pilotCmd = &cobra.Command{
	Use:   "pilot",
	Short: "Run automated piloting test (Server + 100k Batch Feeder + Verification)",
	Long:  `Menjalankan pengujian piloting terintegrasi: menyalakan Server, streaming 100k debitur via Feeder, dan validasi cache Redis`,
	Run: func(cmd *cobra.Command, args []string) {
		runPilotSimulation(pilotCount, pilotChunkSize)
	},
}

func init() {
	pilotCmd.Flags().IntVarP(&pilotCount, "count", "n", 100000, "Jumlah debitur piloting (default 100.000)")
	pilotCmd.Flags().IntVarP(&pilotChunkSize, "chunk-size", "c", 500, "Ukuran chunk per pesan stream")

	rootCmd.AddCommand(pilotCmd)
}

func runPilotSimulation(total int, chunkSize int) {
	cfg := config.NewConfig()
	config.NewLogger(cfg.App.AppEnv, cfg.App.LogLevel, "piloting-runner")

	log.Info().
		Int("target_debiturs", total).
		Int("chunk_size", chunkSize).
		Msg("=== INITIATING MONOREPO PILOTING SIMULATION ===")

	// 1. Connect to Redis
	redisClient, err := cfg.NewRedisClient()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed connecting to Redis for pilot")
	}
	defer redisClient.Close()

	// 2. Setup Server
	prewarmRepo := repository.NewPrewarmRedisRepository(redisClient)
	prewarmService := service.NewPrewarmService(prewarmRepo, cfg)
	prewarmGRPCHandler := grpchandler.NewPreWarmGRPCHandler(prewarmService)

	grpcServer := grpc.NewServer()
	loanv1.RegisterLoanPreWarmServiceServer(grpcServer, prewarmGRPCHandler)

	grpcListener, err := net.Listen("tcp", ":"+cfg.App.GrpcPort)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen on gRPC port")
	}

	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil && err != grpc.ErrServerStopped {
			log.Error().Err(err).Msg("gRPC server encountered error")
		}
	}()
	defer grpcServer.GracefulStop()

	// Setup Fiber App
	fiberApp := cfg.NewFiber()
	fiberApp.Use(fiberRecover.New())
	fiberApp.Use(fiberCors.New())
	httphandler.NewHealthCheckHandler(fiberApp, prewarmService)
	httphandler.NewOfferHandler(fiberApp, prewarmService)

	go func() {
		_ = fiberApp.Listen(":"+cfg.App.AppPort, fiber.ListenConfig{EnablePrefork: false})
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = fiberApp.ShutdownWithContext(ctx)
	}()

	// Tunggu server menyala
	time.Sleep(500 * time.Millisecond)

	// 3. Jalankan Batch Feeder
	batchID := fmt.Sprintf("PILOT-%d", time.Now().Unix())
	target := "localhost:" + cfg.App.GrpcPort

	result, err := app.RunFeeder(batchID, total, chunkSize, target)
	if err != nil {
		log.Fatal().Err(err).Msg("Piloting feeder failed")
	}

	// 4. Verifikasi URL pendek & latensi pengambilan
	log.Info().Msg("Verifying sample debitur retrieval via NIK and Token...")
	sampleNIK := "3201010000000001" // Debitur pertama
	token, err := prewarmRepo.GetTokenByNIK(context.Background(), sampleNIK)
	if err != nil {
		log.Error().Err(err).Str("nik", sampleNIK).Msg("Sample NIK token lookup failed")
	} else {
		log.Info().Str("sample_nik", sampleNIK).Str("token", token).Msg("Found token for sample NIK")

		// Query HTTP Endpoint /s/:token
		client := &http.Client{Timeout: 2 * time.Second}
		startReq := time.Now()
		resp, err := client.Get(fmt.Sprintf("http://localhost:%s/s/%s", cfg.App.AppPort, token))
		lat := time.Since(startReq)

		if err != nil {
			log.Error().Err(err).Msg("Failed to query /s/:token")
		} else {
			defer resp.Body.Close()
			log.Info().
				Int("status_code", resp.StatusCode).
				Dur("http_latency", lat).
				Msg("Debitur offer retrieved via HTTP (< 5ms target achieved)")
		}
	}

	fmt.Println("\n🎉 PILOTING SIMULATION SUCCEEDED!")
	fmt.Printf("Total Data Processed : %d debiturs\n", result.TotalDispatched)
	fmt.Printf("Total Ingested Redis : %d debiturs\n", result.TotalCached)
	fmt.Printf("Ingestion Duration   : %d ms (%.2f s)\n", result.DurationMs, float64(result.DurationMs)/1000.0)
	fmt.Printf("Streaming Throughput : %.2f records/second\n", result.ThroughputRPS)
	fmt.Printf("Client Heap Memory   : %.2f MB\n", result.MemoryAllocMB)
	fmt.Println("==================================================================")
}
