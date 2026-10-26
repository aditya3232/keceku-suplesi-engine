// Package service berisi logika bisnis inti (core business logic) dari aplikasi,
// termasuk pre-warming cache dan batch ingestion feeder.
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

// FeederService mendefinisikan kontrak ingestion data debitur masif ke server.
// Service ini bertugas mengalirkan (streaming) kandidat debitur ke Loan Service
// menggunakan gRPC Client Streaming dengan konsumsi memori yang stabil dan efisiensi
// jaringan tinggi.
type FeederService interface {
	// RunFeeder menjalankan proses batch feeding secara streaming ke gRPC server.
	// Mengembalikan FeederJobResult yang merangkum metrik performa (durasi, throughput, dan alokasi memori).
	RunFeeder(ctx context.Context, cfg model.FeederJobConfig, client loanv1.LoanPreWarmServiceClient) (*model.FeederJobResult, error)
}

// feederService merupakan implementasi konkret dari interface FeederService.
type feederService struct{}

// NewFeederService membuat dan mengembalikan instance baru dari FeederService.
func NewFeederService() FeederService {
	return &feederService{}
}

// RunFeeder mengorkestrasi pengaliran data debitur masif ke Loan Service via gRPC Client Streaming.
//
// Cara Kerja:
//  1. Validasi & Normalisasi Konfigurasi: Memastikan ChunkSize, TotalCount, dan BatchID terdefinisi dengan nilai aman.
//  2. Membuka Koneksi Stream: Menginisialisasi 1 koneksi gRPC Client Stream tunggal (HTTP/2) ke server target.
//  3. Pipeline Generator & Backpressure: Menjalankan goroutine produsen data ke buffered channel (kapasitas 2x ChunkSize).
//     Mekanisme backpressure memastikan pembacaan data otomatis terjeda jika jaringan/server melambat,
//     sehingga heap memory tetap konstan (< 25 MB) tanpa risiko Out of Memory (OOM).
//  4. Batch Chunking: Mengumpulkan item dari channel ke dalam slice berukuran ChunkSize (default: 500)
//     sebelum dikirim sebagai 1 pesan proto PreWarmDebiturChunk untuk meminimalkan network round-trip.
//  5. Flush Tail Data: Mengirimkan sisa item terakhir yang belum genap mencapai ChunkSize.
//  6. Close and Receive Summary: Menutup stream dari sisi client (EOF) dan menerima satu respons ringkasan
//     (PreWarmDebiturSummary) dari server yang berisi total data berhasil di-cache ke Redis.
//  7. Profiling & Metrik: Mengukur throughput (record/detik), durasi pemrosesan, dan penggunaan heap runtime Go.
func (f *feederService) RunFeeder(ctx context.Context, cfg model.FeederJobConfig, client loanv1.LoanPreWarmServiceClient) (*model.FeederJobResult, error) {
	// Langkah 1: Validasi dan set default konfigurasi batch feeder
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

	// Langkah 2: Inisialisasi gRPC Client Stream (1 koneksi HTTP/2 tunggal)
	stream, err := client.StreamPreWarmDebitur(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open gRPC client stream: %w", err)
	}

	// Langkah 3: Inisialisasi bounded buffered channel sebagai pipeline produsen-konsumen.
	// Bounded channel ini memberikan backpressure otomatis agar memory footprint tetap stabil (< 25 MB).
	dataChan := make(chan *loanv1.DebiturCandidate, cfg.ChunkSize*2)
	go generateSyntheticDebiturStream(cfg.TotalCount, dataChan)

	startTime := time.Now()
	chunk := make([]*loanv1.DebiturCandidate, 0, cfg.ChunkSize)
	chunkIndex := 0
	totalDispatched := 0

	// Langkah 4: Konsumsi data dari channel dan kelompokkan per chunk (batching)
	for debitur := range dataChan {
		chunk = append(chunk, debitur)
		totalDispatched++

		// Jika ukuran chunk tercapai, kirimkan paket chunk ke stream
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

			// Alokasikan kembali slice baru dengan kapasitas tetap untuk chunk berikutnya
			chunk = make([]*loanv1.DebiturCandidate, 0, cfg.ChunkSize)
		}
	}

	// Langkah 5: Kirim sisa data terakhir (tail chunk) jika jumlah data bukan kelipatan ChunkSize
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

	// Langkah 6: Tutup stream sisi client dan terima ringkasan respons (summary) dari server
	summary, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("error waiting stream summary from server: %w", err)
	}

	// Langkah 7: Pengukuran metrik performa (durasi, throughput, alokasi memori runtime)
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

// generateSyntheticDebiturStream mensimulasikan pembacaan baris data dari Data Lake / DB Replika secara kursor.
//
// Fungsi ini berjalan secara asinkron pada goroutine terpisah sebagai produsen data.
// Menggunakan channel terarah kirim (chan<-) dan ditutup secara otomatis (close) ketika seluruh data
// selesai digenerate untuk memberi sinyal EOF ke perulangan range penerima.
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
