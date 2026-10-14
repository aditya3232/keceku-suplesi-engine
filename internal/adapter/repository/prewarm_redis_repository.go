package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aditya3232/keceku-suplesi-engine/internal/core/domain/entity"
	"github.com/aditya3232/keceku-suplesi-engine/utils"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type PrewarmRedisRepository interface {
	PipelineSaveBatch(ctx context.Context, records []*entity.DebiturPrewarmRecord, ttl time.Duration) ([]string, error)
	GetDebiturByToken(ctx context.Context, token string) (*entity.DebiturPrewarmRecord, error)
	GetTokenByNIK(ctx context.Context, nik string) (string, error)
	CountCachedKeys(ctx context.Context) (int64, error)
	Ping(ctx context.Context) error
}

type prewarmRedisRepository struct {
	client redis.UniversalClient
}

func NewPrewarmRedisRepository(client redis.UniversalClient) PrewarmRedisRepository {
	return &prewarmRedisRepository{
		client: client,
	}
}

// PipelineSaveBatch mengeksekusi penyimpanan batch debitur ke Redis menggunakan Redis Pipeline dalam 1 round-trip network
func (r *prewarmRedisRepository) PipelineSaveBatch(ctx context.Context, records []*entity.DebiturPrewarmRecord, ttl time.Duration) ([]string, error) {
	if len(records) == 0 {
		return nil, nil
	}

	pipe := r.client.Pipeline()
	var failedNIKs []string

	for _, rec := range records {
		payloadBytes, err := json.Marshal(rec)
		if err != nil {
			failedNIKs = append(failedNIKs, rec.NIK)
			continue
		}

		// 1. Simpan token -> record debitur (O(1) look-up saat debitur klik link WhatsApp)
		keyToken := fmt.Sprintf("%s%s", utils.RedisPrefixToken, rec.Token)
		pipe.Set(ctx, keyToken, payloadBytes, ttl)

		// 2. Simpan nik -> token (indeks balik untuk audit / pembatalan penawaran oleh Mantri)
		keyNIK := fmt.Sprintf("%s%s", utils.RedisPrefixNIK, rec.NIK)
		pipe.Set(ctx, keyNIK, rec.Token, ttl)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Error().
			Err(err).
			Int("batch_size", len(records)).
			Str("source", "repository.PipelineSaveBatch").
			Msg("failed executing redis pipeline batch")

		// Jika seluruh pipeline gagal, tandai semua NIK dalam batch ini
		failedAll := make([]string, 0, len(records))
		for _, rec := range records {
			failedAll = append(failedAll, rec.NIK)
		}
		return failedAll, err
	}

	return failedNIKs, nil
}

// GetDebiturByToken mengambil data debitur dari Redis berdasarkan token dengan target latensi < 5ms
func (r *prewarmRedisRepository) GetDebiturByToken(ctx context.Context, token string) (*entity.DebiturPrewarmRecord, error) {
	key := fmt.Sprintf("%s%s", utils.RedisPrefixToken, token)
	val, err := r.client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var record entity.DebiturPrewarmRecord
	if err := json.Unmarshal(val, &record); err != nil {
		return nil, err
	}

	return &record, nil
}

// GetTokenByNIK mengambil token suplesi berdasarkan NIK debitur
func (r *prewarmRedisRepository) GetTokenByNIK(ctx context.Context, nik string) (string, error) {
	key := fmt.Sprintf("%s%s", utils.RedisPrefixNIK, nik)
	return r.client.Get(ctx, key).Result()
}

// CountCachedKeys menghitung total key token suplesi yang aktif di Redis
func (r *prewarmRedisRepository) CountCachedKeys(ctx context.Context) (int64, error) {
	// Menggunakan DBSize untuk cek cepat
	return r.client.DBSize(ctx).Result()
}

// Ping memverifikasi koneksi ke Redis
func (r *prewarmRedisRepository) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}
