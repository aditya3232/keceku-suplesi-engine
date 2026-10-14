package utils

import "time"

const (
	RedisPrefixToken = "keceku:token:"
	RedisPrefixNIK   = "keceku:nik:"

	DefaultChunkSize = 500
	DefaultTTL       = 48 * time.Hour
)
