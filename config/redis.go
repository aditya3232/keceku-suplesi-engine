package config

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

var Ctx = context.Background()

func (cfg Config) NewRedisClient() (redis.UniversalClient, error) {
	var client redis.UniversalClient

	if cfg.Redis.ClusterMode {
		addrs := cfg.Redis.ClusterAddrs
		if len(addrs) == 0 {
			connect := fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port)
			addrs = []string{connect}
		}

		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			Password:     cfg.Redis.Password,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
	} else {
		connect := fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port)
		client = redis.NewClient(&redis.Options{
			Addr:         connect,
			Password:     cfg.Redis.Password,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
	}

	ctx, cancel := context.WithTimeout(Ctx, 5*time.Second)
	defer cancel()

	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Error().
			Err(err).
			Str("source", "config.NewRedisClient").
			Msg("Failed to ping redis client")
		return nil, err
	}

	log.Info().
		Str("host", cfg.Redis.Host).
		Str("port", cfg.Redis.Port).
		Bool("cluster_mode", cfg.Redis.ClusterMode).
		Msg("Redis client connected successfully")

	return client, nil
}
