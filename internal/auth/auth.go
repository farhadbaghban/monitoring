package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
)

// Hash returns the hex SHA-256 of a key. Only hashes are compared or stored.
func Hash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Equal compares two hashes in constant time.
func Equal(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
