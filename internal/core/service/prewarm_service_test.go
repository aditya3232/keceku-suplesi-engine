package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/config"
	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/domain/entity"
)

type mockPrewarmRepo struct {
	records map[string]*entity.DebiturPrewarmRecord
	tokens  map[string]string // NIK -> Token
}

func newMockRepo() *mockPrewarmRepo {
	return &mockPrewarmRepo{
		records: make(map[string]*entity.DebiturPrewarmRecord),
		tokens:  make(map[string]string),
	}
}

func (m *mockPrewarmRepo) PipelineSaveBatch(ctx context.Context, records []*entity.DebiturPrewarmRecord, ttl time.Duration) ([]string, error) {
	for _, r := range records {
		m.records[r.Token] = r
		m.tokens[r.NIK] = r.Token
	}
	return nil, nil
}

func (m *mockPrewarmRepo) GetDebiturByToken(ctx context.Context, token string) (*entity.DebiturPrewarmRecord, error) {
	if r, ok := m.records[token]; ok {
		return r, nil
	}
	return nil, fmt.Errorf("not found")
}

func (m *mockPrewarmRepo) GetTokenByNIK(ctx context.Context, nik string) (string, error) {
	if t, ok := m.tokens[nik]; ok {
		return t, nil
	}
	return "", fmt.Errorf("not found")
}

func (m *mockPrewarmRepo) CountCachedKeys(ctx context.Context) (int64, error) {
	return int64(len(m.records)), nil
}

func (m *mockPrewarmRepo) Ping(ctx context.Context) error {
	return nil
}

func TestProcessChunkAndRetrieveOffer(t *testing.T) {
	mockRepo := newMockRepo()
	cfg := &config.Config{
		Redis: config.Redis{TTLHour: 48},
	}
	svc := NewPrewarmService(mockRepo, cfg)

	candidates := []*loanv1.DebiturCandidate{
		{
			Nik:               "3201010000000001",
			Nama:              "Budi Santoso",
			NomorRekening:     "012301000001",
			NomorTelepon:      "08123456789",
			UnitKerjaId:       "UNIT-BRI-001",
			ExistingBakiDebet: 15_000_000,
			OriginalPlafond:   50_000_000,
			MaxSuplesiAmount:  35_000_000,
			AllowedTenors:     []int32{12, 24, 36},
		},
		{
			Nik:               "3201010000000002",
			Nama:              "Siti Aminah",
			NomorRekening:     "012301000002",
			NomorTelepon:      "08129876543",
			UnitKerjaId:       "UNIT-BRI-002",
			ExistingBakiDebet: 20_000_000,
			OriginalPlafond:   40_000_000,
			MaxSuplesiAmount:  20_000_000,
			AllowedTenors:     []int32{12, 24},
		},
	}

	ctx := context.Background()
	cached, failed, failedNIKs, err := svc.ProcessChunk(ctx, "BATCH-TEST-1", candidates)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cached != 2 {
		t.Errorf("expected 2 cached, got %d", cached)
	}
	if failed != 0 {
		t.Errorf("expected 0 failed, got %d", failed)
	}
	if len(failedNIKs) > 0 {
		t.Errorf("expected empty failedNIKs, got %v", failedNIKs)
	}

	// Verify reverse token lookup
	token, err := mockRepo.GetTokenByNIK(ctx, "3201010000000001")
	if err != nil {
		t.Fatalf("failed to get token by NIK: %v", err)
	}
	if token == "" {
		t.Fatal("token should not be empty")
	}

	// Verify Offer retrieval by token
	offer, err := svc.GetOfferingByToken(ctx, token)
	if err != nil {
		t.Fatalf("failed to get offering: %v", err)
	}
	if offer.Nama != "Budi Santoso" {
		t.Errorf("expected Nama 'Budi Santoso', got '%s'", offer.Nama)
	}
	if offer.NomorRekeningMask != "012-****-001" {
		t.Errorf("expected masked rekening '012-****-001', got '%s'", offer.NomorRekeningMask)
	}
	if offer.MaxSuplesiAmount != 35_000_000 {
		t.Errorf("expected max suplesi 35000000, got %d", offer.MaxSuplesiAmount)
	}
}

func BenchmarkProcessChunk(b *testing.B) {
	mockRepo := newMockRepo()
	cfg := &config.Config{
		Redis: config.Redis{TTLHour: 48},
	}
	svc := NewPrewarmService(mockRepo, cfg)

	// Chunk 500 items
	chunkSize := 500
	candidates := make([]*loanv1.DebiturCandidate, chunkSize)
	for i := 0; i < chunkSize; i++ {
		candidates[i] = &loanv1.DebiturCandidate{
			Nik:               fmt.Sprintf("320101%010d", i),
			Nama:              "Debitur Benchmark",
			NomorRekening:     "012301000001",
			NomorTelepon:      "08123456789",
			UnitKerjaId:       "UNIT-001",
			ExistingBakiDebet: 20_000_000,
			OriginalPlafond:   50_000_000,
			MaxSuplesiAmount:  30_000_000,
			AllowedTenors:     []int32{12, 24, 36},
		}
	}

	ctx := context.Background()

	for b.Loop() {
		_, _, _, _ = svc.ProcessChunk(ctx, "BENCH-BATCH", candidates)
	}
}
