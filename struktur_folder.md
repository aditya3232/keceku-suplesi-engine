```bash
.
├── cmd/                                  # Cobra CLI Commands & Subcommands
│   ├── root.go                           # Root command & inisialisasi Viper .env
│   ├── start.go                          # Menjalankan Server gRPC + HTTP (alias: server)
│   ├── feeder.go                         # Menjalankan Batch Feeder Client Streaming
│   └── pilot.go                          # Piloting otomatis: Server + 100k Feeder + Validasi Cache
├── config/                               # Konfigurasi aplikasi (Viper)
│   ├── config.go                         # Struct App, Redis, dan Feeder config
│   ├── fiber.go                          # Fiber v3 app & standard error handler
│   ├── redis.go                          # UniversalClient (Standalone & Redis Cluster)
│   └── zerolog.go                        # Zerolog (warna konsol dev / format JSON prod)
├── gen/                                  # Kode Go hasil kompilasi Protobuf
│   └── loan/v1/
│       ├── loan_prewarm.pb.go
│       └── loan_prewarm_grpc.pb.go
├── internal/
│   ├── adapter/
│   │   ├── handler/
│   │   │   ├── grpc/
│   │   │   │   └── prewarm_grpc_handler.go  # gRPC Stream Handler (Client Streaming)
│   │   │   ├── http/
│   │   │   │   ├── health_check_handler.go  # Endpoint /health & /api/check
│   │   │   │   └── offer_handler.go         # Endpoint /s/:token (Simulasi klik WA debitur)
│   │   │   └── response/
│   │   │       └── default_response.go   # Format standar DefaultResponse
│   │   └── repository/
│   │       └── prewarm_redis_repository.go  # Redis Pipeline batching & direct key lookup
│   ├── app/
│   │   ├── app.go                        # Server lifecycle, wire-up, & graceful shutdown
│   │   └── feeder_app.go                 # Feeder client runner & memory profiling
│   └── core/
│       ├── domain/
│       │   ├── entity/
│       │   │   └── debitur_entity.go     # Debitur & Suplesi Offer Entity
│       │   └── model/
│       │       └── prewarm_model.go      # Model DTO & Feeder config
│       └── service/
│           ├── feeder_service.go         # Streaming generator & dispatcher
│           ├── prewarm_service.go        # Chunk processing, token gen, & Redis batching
│           └── prewarm_service_test.go   # Unit test & benchmark throughput
├── proto/
│   └── keceku/loan/v1/
│       └── loan_prewarm.proto            # Definisi kontrak gRPC Client Streaming
├── utils/
│   ├── constant.go                       # Redis key prefixes & default TTL (48 jam)
│   └── token.go                          # Cryptographic URL-safe token generator
├── .env.example                          # Template konfigurasi environment
├── .gitignore
├── Makefile                              # Otomasi proto, server, feeder, dan pilot
├── README.md                             # Dokumentasi lengkap arsitektur & eksekusi
└── main.go                               # Main entry point (cmd.Execute())
```