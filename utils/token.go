package utils

import (
	"crypto/rand"
	"encoding/hex"
)

// GenerateSecureToken menghasilkan token acak 16 byte (32 karakter hex) yang URL-safe
func GenerateSecureToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
