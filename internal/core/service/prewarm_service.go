package service

import (
	"context"
	"fmt"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/config"
	loanv1 "github.com/aditya3232/keceku-suplesi-engine/gen/loan/v1"
	"github.com/aditya3232/keceku-suplesi-engine/internal/adapter/repository"
	"github.com/aditya3232/keceku-suplesi-engine/internal/core/domain/entity"
	"github.com/aditya3232/keceku-suplesi-engine/utils"
	"github.com/rs/zerolog/log"
)

type PrewarmService interface {
	ProcessChunk(ctx context.Context, batchID string, candidates []*loanv1.DebiturCandidate) (cachedCount int64, failedCount int64, failedNIKs []string, err error)
	GetOfferingByToken(ctx context.Context, token string) (*entity.DebiturOfferingResponse, error)
	GetTokenByNIK(ctx context.Context, nik string) (string, error)
	CheckHealth(ctx context.Context) error
	GetTotalCached(ctx context.Context) (int64, error)
}

type prewarmService struct {
	repo repository.PrewarmRedisRepository
	cfg  *config.Config
	ttl  time.Duration
}

func NewPrewarmService(repo repository.PrewarmRedisRepository, cfg *config.Config) PrewarmService {
	ttl := time.Duration(cfg.Redis.TTLHour) * time.Hour
	if ttl <= 0 {
		ttl = utils.DefaultTTL
	}

	return &prewarmService{
		repo: repo,
		cfg:  cfg,
		ttl:  ttl,
	}
}

// ProcessChunk memproses dan memvalidasi sekumpulan debitur dari stream dan menyimpannya ke Redis secara pipelined
// validasi yg bisa dilakukan:
// - plafond awal Harus bernilai positif (> 0) dan berada dalam rentang limit produk Kupedes (misal Rp 1 Juta s.d. Rp 500 Juta).
// - sisa pinjaman tidak boleh melebihi plafond awal
// - plafon tambahan tidak melebihi aturan
// - piihan tenor sesuai aturan
func (s *prewarmService) ProcessChunk(ctx context.Context, batchID string, candidates []*loanv1.DebiturCandidate) (int64, int64, []string, error) {
	if len(candidates) == 0 {
		return 0, 0, nil, nil
	}

	records := make([]*entity.DebiturPrewarmRecord, 0, len(candidates))
	var failedNIKs []string

	now := time.Now()
	for _, c := range candidates {
		token, err := utils.GenerateSecureToken()
		if err != nil {
			log.Error().
				Err(err).
				Str("nik", c.GetNik()).
				Msg("failed generating token for debitur")
			failedNIKs = append(failedNIKs, c.GetNik())
			continue
		}

		records = append(records, &entity.DebiturPrewarmRecord{
			NIK:               c.GetNik(),
			Nama:              c.GetNama(),
			NomorRekening:     c.GetNomorRekening(),
			NomorTelepon:      c.GetNomorTelepon(),
			UnitKerjaID:       c.GetUnitKerjaId(),
			ExistingBakiDebet: c.GetExistingBakiDebet(),
			OriginalPlafond:   c.GetOriginalPlafond(),
			MaxSuplesiAmount:  c.GetMaxSuplesiAmount(),
			AllowedTenors:     c.GetAllowedTenors(),
			Token:             token,
			CreatedAt:         now,
		})
	}

	// Eksekusi ke Redis repository dalam 1 round-trip Pipeline
	pipelineFailedNIKs, err := s.repo.PipelineSaveBatch(ctx, records, s.ttl)
	if err != nil {
		return 0, int64(len(candidates)), pipelineFailedNIKs, err
	}

	failedNIKs = append(failedNIKs, pipelineFailedNIKs...)
	cachedCount := int64(len(records) - len(pipelineFailedNIKs))
	failedCount := int64(len(failedNIKs))

	return cachedCount, failedCount, failedNIKs, nil
}

// GetOfferingByToken menyajikan data penawaran suplesi saat debitur membuka URL unik dari SMS/WhatsApp Blast
func (s *prewarmService) GetOfferingByToken(ctx context.Context, token string) (*entity.DebiturOfferingResponse, error) {
	record, err := s.repo.GetDebiturByToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("token invalid atau telah kedaluwarsa: %w", err)
	}

	// Masking nomor rekening untuk privasi keamanan (contoh: 0123-****-5678)
	maskedRek := maskAccountNumber(record.NomorRekening)
	expiryTime := record.CreatedAt.Add(s.ttl).Format(time.RFC3339)

	return &entity.DebiturOfferingResponse{
		Token:             token,
		Nama:              record.Nama,
		NomorRekeningMask: maskedRek,
		MaxSuplesiAmount:  record.MaxSuplesiAmount,
		ExistingBakiDebet: record.ExistingBakiDebet,
		AllowedTenors:     record.AllowedTenors,
		Expiry:            expiryTime,
	}, nil
}

func (s *prewarmService) CheckHealth(ctx context.Context) error {
	return s.repo.Ping(ctx)
}

func (s *prewarmService) GetTotalCached(ctx context.Context) (int64, error) {
	return s.repo.CountCachedKeys(ctx)
}

func (s *prewarmService) GetTokenByNIK(ctx context.Context, nik string) (string, error) {
	return s.repo.GetTokenByNIK(ctx, nik)
}

func maskAccountNumber(accountNum string) string {
	if len(accountNum) <= 6 {
		return accountNum
	}
	prefix := accountNum[:3]
	suffix := accountNum[len(accountNum)-3:]
	return fmt.Sprintf("%s-****-%s", prefix, suffix)
}
