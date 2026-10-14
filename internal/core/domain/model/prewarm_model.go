package model

type HealthStatus struct {
	Status    string `json:"status"`
	Redis     string `json:"redis"`
	Timestamp string `json:"timestamp"`
}

type FeederJobConfig struct {
	BatchID          string `json:"batch_id"`
	TotalCount       int    `json:"total_count"`
	ChunkSize        int    `json:"chunk_size"`
	ServerGrpcTarget string `json:"server_grpc_target"`
}

type FeederJobResult struct {
	BatchID         string  `json:"batch_id"`
	TotalDispatched int     `json:"total_dispatched"`
	TotalCached     int64   `json:"total_cached"`
	TotalFailed     int64   `json:"total_failed"`
	DurationMs      int64   `json:"duration_ms"`
	ThroughputRPS   float64 `json:"throughput_rps"`
	MemoryAllocMB   float64 `json:"memory_alloc_mb"`
}
