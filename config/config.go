package config

import (
	"strings"

	"github.com/spf13/viper"
)

type App struct {
	AppPort    string `json:"app_port"`
	GrpcPort   string `json:"grpc_port"`
	AppEnv     string `json:"app_env"`
	AppName    string `json:"app_name"`
	WebPrefork bool   `json:"web_prefork"`
	LogLevel   string `json:"log_level"`
}

type Redis struct {
	Host         string   `json:"host"`
	Port         string   `json:"port"`
	Password     string   `json:"password"`
	ClusterMode  bool     `json:"cluster_mode"`
	ClusterAddrs []string `json:"cluster_addrs"`
	TTLHour      int      `json:"ttl_hour"`
}

type Feeder struct {
	BatchSize        int    `json:"batch_size"`
	TotalCandidates  int    `json:"total_candidates"`
	ServerGrpcTarget string `json:"server_grpc_target"`
}

type Config struct {
	App    App    `json:"app"`
	Redis  Redis  `json:"redis"`
	Feeder Feeder `json:"feeder"`
}

func NewConfig() *Config {
	ttlHour := viper.GetInt("REDIS_TTL_HOUR")
	if ttlHour <= 0 {
		ttlHour = 48
	}

	appPort := viper.GetString("APP_PORT")
	if appPort == "" {
		appPort = "8090"
	}

	grpcPort := viper.GetString("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	batchSize := viper.GetInt("FEEDER_BATCH_SIZE")
	if batchSize <= 0 {
		batchSize = 500
	}

	totalCandidates := viper.GetInt("FEEDER_TOTAL_CANDIDATES")
	if totalCandidates <= 0 {
		totalCandidates = 100000
	}

	serverGrpcTarget := viper.GetString("FEEDER_GRPC_TARGET")
	if serverGrpcTarget == "" {
		serverGrpcTarget = "localhost:" + grpcPort
	}

	clusterAddrsStr := viper.GetString("REDIS_CLUSTER_ADDRS")
	var clusterAddrs []string
	if clusterAddrsStr != "" {
		clusterAddrs = strings.Split(clusterAddrsStr, ",")
	}

	return &Config{
		App: App{
			AppPort:    appPort,
			GrpcPort:   grpcPort,
			AppEnv:     viper.GetString("APP_ENV"),
			AppName:    viper.GetString("APP_NAME"),
			WebPrefork: viper.GetBool("WEB_PREFORK"),
			LogLevel:   viper.GetString("LOG_LEVEL"),
		},
		Redis: Redis{
			Host:         viper.GetString("REDIS_HOST"),
			Port:         viper.GetString("REDIS_PORT"),
			Password:     viper.GetString("REDIS_PASSWORD"),
			ClusterMode:  viper.GetBool("REDIS_CLUSTER_MODE"),
			ClusterAddrs: clusterAddrs,
			TTLHour:      ttlHour,
		},
		Feeder: Feeder{
			BatchSize:        batchSize,
			TotalCandidates:  totalCandidates,
			ServerGrpcTarget: serverGrpcTarget,
		},
	}
}
