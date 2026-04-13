package polymarket

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"time"
)

// L2Headers generates the HMAC-signed auth headers for a Polymarket API request.
func L2Headers(apiKey, apiSecret, passphrase, address, method, path string, body []byte) (map[string]string, error) {
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	return l2HeadersWithTimestamp(apiKey, apiSecret, passphrase, address, method, path, body, timestamp)
}

func l2HeadersWithTimestamp(apiKey, apiSecret, passphrase, address, method, path string, body []byte, timestamp string) (map[string]string, error) {
	message := timestamp + "\n" + method + "\n" + path + "\n" + string(body)

	// Pad secret to valid base64
	padded := apiSecret
	for len(padded)%4 != 0 {
		padded += "="
	}
	secretBytes, err := base64.URLEncoding.DecodeString(padded)
	if err != nil {
		return nil, fmt.Errorf("decode api secret: %w", err)
	}

	mac := hmac.New(sha256.New, secretBytes)
	mac.Write([]byte(message))
	sig := base64.URLEncoding.EncodeToString(mac.Sum(nil))

	return map[string]string{
		"POLY_API_KEY":    apiKey,
		"POLY_SIGNATURE":  sig,
		"POLY_TIMESTAMP":  timestamp,
		"POLY_PASSPHRASE": passphrase,
		"POLY_ADDRESS":    address,
	}, nil
}
