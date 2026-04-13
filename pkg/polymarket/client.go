package polymarket

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
)

// Client talks to the Polymarket CLOB API.
type Client struct {
	cfg             Config
	httpClient      *http.Client
	privKey         *secp256k1.PrivateKey
	domainSeparator []byte
}

// NewClient creates a Polymarket client. Uses NegRisk exchange by default.
func NewClient(cfg Config) (*Client, error) {
	privKey, err := parsePrivateKey(cfg.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return &Client{
		cfg:             cfg,
		httpClient:      &http.Client{},
		privKey:         privKey,
		domainSeparator: buildDomainSeparator(cfg.ChainID, NegRiskExchange),
	}, nil
}

// BuildOrderRequest creates a signed SendOrderRequest from record spec fields.
func (c *Client) BuildOrderRequest(tokenID, side, price, size, orderType, expiration string, deferExec bool) (*SendOrderRequest, error) {
	makerAmount, takerAmount, err := computeAmounts(side, price, size)
	if err != nil {
		return nil, err
	}

	signer := c.cfg.Address
	maker := c.cfg.Funder
	if maker == "" {
		maker = signer
	}

	order := &OrderPayload{
		Salt:          randomSalt(),
		Maker:         maker,
		Signer:        signer,
		Taker:         ZeroAddress,
		TokenID:       tokenID,
		MakerAmount:   makerAmount,
		TakerAmount:   takerAmount,
		Expiration:    expiration,
		Nonce:         "0",
		FeeRateBps:    "0",
		Side:          side,
		SignatureType: c.cfg.SignatureType,
	}

	structHash, err := buildOrderStructHash(order)
	if err != nil {
		return nil, fmt.Errorf("build struct hash: %w", err)
	}

	rawSig, err := signOrder(c.domainSeparator, structHash, c.privKey)
	if err != nil {
		return nil, fmt.Errorf("sign order: %w", err)
	}

	sigHex := "0x" + hex.EncodeToString(rawSig)
	if c.cfg.SignatureType == 1 { // POLY_PROXY: prepend signer address
		signerAddr := strings.ToLower(strings.TrimPrefix(signer, "0x"))
		sigHex = "0x" + signerAddr + hex.EncodeToString(rawSig)
	}
	order.Signature = sigHex

	return &SendOrderRequest{
		Order:     *order,
		Owner:     c.cfg.APIKey,
		OrderType: orderType,
		DeferExec: deferExec,
	}, nil
}

// PostOrder submits a signed order to the Polymarket CLOB.
func (c *Client) PostOrder(ctx context.Context, req *SendOrderRequest) (*SendOrderResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	path := "/order"
	url := strings.TrimRight(c.cfg.Host, "/") + path

	headers, err := L2Headers(c.cfg.APIKey, c.cfg.APISecret, c.cfg.APIPassphrase, c.cfg.Address, "POST", path, body)
	if err != nil {
		return nil, fmt.Errorf("build auth headers: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		httpReq.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("POST /order: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("POST /order returned %d: %s", resp.StatusCode, string(respBody))
	}

	var result SendOrderResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}
	return &result, nil
}
