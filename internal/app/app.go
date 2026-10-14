package app

import (
	"context"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/config"
	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
	grpchandler "github.com/aditya3232/keceku-suplesi-engine/internal/adapter/handler/grpc"
	httphandler "github.com/aditya3232/keceku-suplesi-engine/internal/adapter/handler/http"
	"github.com/aditya3232/keceku-suplesi-engine/internal/adapter/repository"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/service"
	"github.com/gofiber/fiber/v3"
	fiberCors "github.com/gofiber/fiber/v3/middleware/cors"
	fiberRecover "github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func RunServer() {
	cfg := config.NewConfig()

	config.NewLogger(cfg.App.AppEnv, cfg.App.LogLevel, cfg.App.AppName)

	log.Info().
		Str("app_name", cfg.App.AppName).
		Str("app_env", cfg.App.AppEnv).
		Str("grpc_port", cfg.App.GrpcPort).
		Str("http_port", cfg.App.AppPort).
		Msg("Initializing Keceku Suplesi Engine Server")

	// 1. Connect to Redis (Universal Client: Standalone / Cluster)
	redisClient, err := cfg.NewRedisClient()
	if err != nil {
		log.Fatal().
			Err(err).
			Str("source", "app.RunServer").
			Msg("failed connect to redis")
	}

	// 2. Initialize Layering (Repository -> Service -> Handlers)
	prewarmRepo := repository.NewPrewarmRedisRepository(redisClient)
	prewarmService := service.NewPrewarmService(prewarmRepo, cfg)
	prewarmGRPCHandler := grpchandler.NewPreWarmGRPCHandler(prewarmService)

	// 3. Setup gRPC Server
	grpcServer := grpc.NewServer()
	loanv1.RegisterLoanPreWarmServiceServer(grpcServer, prewarmGRPCHandler)
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", ":"+cfg.App.GrpcPort)
	if err != nil {
		log.Fatal().
			Err(err).
			Str("port", cfg.App.GrpcPort).
			Str("source", "app.RunServer").
			Msg("failed to listen on gRPC port")
	}

	go func() {
		log.Info().
			Str("grpc_port", ":"+cfg.App.GrpcPort).
			Str("source", "app.RunServer").
			Msg("gRPC LoanPreWarmService started")

		if err := grpcServer.Serve(grpcListener); err != nil && err != grpc.ErrServerStopped {
			log.Fatal().
				Err(err).
				Str("source", "app.RunServer").
				Msg("failed starting gRPC server")
		}
	}()

	// 4. Setup Fiber HTTP Server for Health Checks & Token Inspection
	fiberApp := cfg.NewFiber()
	fiberApp.Use(fiberRecover.New())
	fiberApp.Use(fiberCors.New())

	httphandler.NewHealthCheckHandler(fiberApp, prewarmService)
	httphandler.NewOfferHandler(fiberApp, prewarmService)

	go func() {
		port := ":" + cfg.App.AppPort
		log.Info().
			Str("http_port", port).
			Str("source", "app.RunServer").
			Msg("Fiber HTTP Server started")

		if err := fiberApp.Listen(port, fiber.ListenConfig{EnablePrefork: cfg.App.WebPrefork}); err != nil {
			log.Fatal().
				Err(err).
				Str("source", "app.RunServer").
				Msg("failed starting HTTP server")
		}
	}()

	// 5. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	<-quit

	log.Info().
		Str("source", "app.RunServer").
		Msg("Shutting down Keceku Suplesi Engine server gracefully...")

	// Stop gRPC server
	grpcServer.GracefulStop()
	log.Info().Msg("gRPC server stopped")

	// Shutdown Fiber app with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := fiberApp.ShutdownWithContext(ctx); err != nil {
		log.Error().
			Err(err).
			Str("source", "app.RunServer").
			Msg("Failed shutdown HTTP server")
	}

	_ = redisClient.Close()
	log.Info().Msg("Redis connection closed. Server stopped gracefully.")
}
