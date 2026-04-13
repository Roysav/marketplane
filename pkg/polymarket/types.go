package polymarket

import "encoding/json"

// Config holds Polymarket API credentials and signing parameters.
type Config struct {
	Host          string // POLY_HOST (e.g. "https://clob.polymarket.com")
	ChainID       int64  // POLY_CHAIN_ID (default 137 for Polygon)
	Address       string // POLY_ADDRESS — wallet/signer address, used in L2 headers and as order.signer
	MakerAddress  string // POLY_MAKER_ADDRESS — unused in MVP, kept for forward compat
	PrivateKey    string // POLY_PRIVATE_KEY — hex-encoded, with or without 0x prefix
	APIKey        string // POLY_API_KEY — also used as POST /order.owner
	APISecret     string // POLY_API_SECRET — base64url-encoded HMAC key
	APIPassphrase string // POLY_API_PASSPHRASE
	SignatureType int    // POLY_SIGNATURE_TYPE (0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE)
	Funder        string // POLY_FUNDER — used as order.maker
}

// SendOrderRequest is the POST /order request body.
type SendOrderRequest struct {
	Order     OrderPayload `json:"order"`
	Owner     string       `json:"owner"`
	OrderType string       `json:"orderType"`
	DeferExec bool         `json:"deferExec,omitempty"`
}

// OrderPayload is the signed order within the request.
type OrderPayload struct {
	Salt          json.Number `json:"salt"`
	Maker         string      `json:"maker"`
	Signer        string      `json:"signer"`
	Taker         string      `json:"taker"`
	TokenID       string      `json:"tokenId"`
	MakerAmount   string      `json:"makerAmount"`
	TakerAmount   string      `json:"takerAmount"`
	Expiration    string      `json:"expiration"`
	Nonce         string      `json:"nonce"`
	FeeRateBps    string      `json:"feeRateBps"`
	Side          string      `json:"side"`
	SignatureType int         `json:"signatureType"`
	Signature     string      `json:"signature"`
}

// SendOrderResponse is the POST /order response (200 OK).
type SendOrderResponse struct {
	Success            bool     `json:"success"`
	OrderID            string   `json:"orderID"`
	Status             string   `json:"status"`
	MakingAmount       string   `json:"makingAmount"`
	TakingAmount       string   `json:"takingAmount"`
	TransactionsHashes []string `json:"transactionsHashes"`
	TradeIDs           []string `json:"tradeIDs"`
	ErrorMsg           string   `json:"errorMsg"`
}
