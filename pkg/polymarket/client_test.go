package polymarket

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Hardhat account #0 — safe to use in tests.
const (
	testPrivateKey = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	testAddress    = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
	testAPIKey     = "test-api-key-uuid"
	testFunder     = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
)

var testSecret = base64.URLEncoding.EncodeToString([]byte("test-secret-key-32bytes-padding!"))

func testConfig(host string) Config {
	return Config{
		Host:          host,
		ChainID:       137,
		Address:       testAddress,
		PrivateKey:    testPrivateKey,
		APIKey:        testAPIKey,
		APISecret:     testSecret,
		APIPassphrase: "test-passphrase",
		SignatureType: 1, // POLY_PROXY
		Funder:        testFunder,
	}
}

// --- Amount computation ---

func TestComputeAmounts_Buy(t *testing.T) {
	// Buy 5 tokens at $0.01 each → cost 0.05 USDC
	// rawPrice = 0.01 * 1e6 = 10000
	// rawSize  = 5 * 1e6    = 5000000
	// notional = 5000000 * 10000 / 1e6 = 50000
	maker, taker, err := computeAmounts("BUY", "0.01", "5")
	if err != nil {
		t.Fatal(err)
	}
	if maker != "50000" {
		t.Errorf("makerAmount = %s, want 50000", maker)
	}
	if taker != "5000000" {
		t.Errorf("takerAmount = %s, want 5000000", taker)
	}
}

func TestComputeAmounts_Buy_Fractional(t *testing.T) {
	// Buy 10 tokens at $0.57 → cost 5.70 USDC
	maker, taker, err := computeAmounts("BUY", "0.57", "10")
	if err != nil {
		t.Fatal(err)
	}
	if maker != "5700000" {
		t.Errorf("makerAmount = %s, want 5700000", maker)
	}
	if taker != "10000000" {
		t.Errorf("takerAmount = %s, want 10000000", taker)
	}
}

func TestComputeAmounts_Sell(t *testing.T) {
	// Sell 10 tokens at $0.57 → receive 5.70 USDC
	maker, taker, err := computeAmounts("SELL", "0.57", "10")
	if err != nil {
		t.Fatal(err)
	}
	if maker != "10000000" {
		t.Errorf("makerAmount = %s, want 10000000", maker)
	}
	if taker != "5700000" {
		t.Errorf("takerAmount = %s, want 5700000", taker)
	}
}

// --- BuildOrderRequest ---

func TestBuildOrderRequest_OwnerIsAPIKey(t *testing.T) {
	client, err := NewClient(testConfig("https://clob.polymarket.com"))
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("12345", "BUY", "0.57", "10", "GTC", "0", false)
	if err != nil {
		t.Fatal(err)
	}

	if req.Owner != testAPIKey {
		t.Errorf("owner = %q, want %q", req.Owner, testAPIKey)
	}
}

func TestBuildOrderRequest_MakerIsFunder(t *testing.T) {
	client, err := NewClient(testConfig("https://clob.polymarket.com"))
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("12345", "BUY", "0.57", "10", "GTC", "0", false)
	if err != nil {
		t.Fatal(err)
	}

	if req.Order.Maker != testFunder {
		t.Errorf("maker = %q, want %q", req.Order.Maker, testFunder)
	}
}

func TestBuildOrderRequest_SignerIsAddress(t *testing.T) {
	client, err := NewClient(testConfig("https://clob.polymarket.com"))
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("12345", "BUY", "0.57", "10", "GTC", "0", false)
	if err != nil {
		t.Fatal(err)
	}

	if req.Order.Signer != testAddress {
		t.Errorf("signer = %q, want %q", req.Order.Signer, testAddress)
	}
}

func TestBuildOrderRequest_Fields(t *testing.T) {
	client, err := NewClient(testConfig("https://clob.polymarket.com"))
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("99999", "SELL", "0.45", "20", "FOK", "1700000000", true)
	if err != nil {
		t.Fatal(err)
	}

	if req.Order.TokenID != "99999" {
		t.Errorf("tokenId = %q, want 99999", req.Order.TokenID)
	}
	if req.Order.Side != "SELL" {
		t.Errorf("side = %q, want SELL", req.Order.Side)
	}
	if req.Order.Taker != ZeroAddress {
		t.Errorf("taker = %q, want zero address", req.Order.Taker)
	}
	if req.Order.Expiration != "1700000000" {
		t.Errorf("expiration = %q, want 1700000000", req.Order.Expiration)
	}
	if req.OrderType != "FOK" {
		t.Errorf("orderType = %q, want FOK", req.OrderType)
	}
	if !req.DeferExec {
		t.Error("deferExec = false, want true")
	}
	if req.Order.Signature == "" {
		t.Error("signature is empty")
	}
	if req.Order.SignatureType != 1 {
		t.Errorf("signatureType = %d, want 1", req.Order.SignatureType)
	}
}

func TestBuildOrderRequest_PolyProxySignatureFormat(t *testing.T) {
	cfg := testConfig("https://clob.polymarket.com")
	cfg.SignatureType = 1
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("12345", "BUY", "0.57", "10", "GTC", "0", false)
	if err != nil {
		t.Fatal(err)
	}

	// POLY_PROXY sig = 0x + signer_address(40 hex) + ecdsa_sig(130 hex) = 172 chars
	sig := req.Order.Signature
	if len(sig) != 172 {
		t.Errorf("POLY_PROXY signature length = %d, want 172", len(sig))
	}
	if sig[:2] != "0x" {
		t.Errorf("signature should start with 0x, got %s", sig[:2])
	}
}

func TestBuildOrderRequest_EOASignatureFormat(t *testing.T) {
	cfg := testConfig("https://clob.polymarket.com")
	cfg.SignatureType = 0
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("12345", "BUY", "0.57", "10", "GTC", "0", false)
	if err != nil {
		t.Fatal(err)
	}

	// EOA sig = 0x + ecdsa_sig(130 hex) = 132 chars
	sig := req.Order.Signature
	if len(sig) != 132 {
		t.Errorf("EOA signature length = %d, want 132", len(sig))
	}
}

// --- PostOrder with mock server ---

func TestPostOrder_Success(t *testing.T) {
	var capturedBody []byte
	var capturedHeaders http.Header

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(SendOrderResponse{
			Success:  true,
			OrderID:  "0xabc123",
			Status:   "live",
			ErrorMsg: "",
		})
	}))
	defer srv.Close()

	client, err := NewClient(testConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("12345", "BUY", "0.01", "5", "GTC", "0", false)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.PostOrder(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if !resp.Success {
		t.Error("expected success=true")
	}
	if resp.OrderID != "0xabc123" {
		t.Errorf("orderID = %q, want 0xabc123", resp.OrderID)
	}
	if resp.Status != "live" {
		t.Errorf("status = %q, want live", resp.Status)
	}

	// Verify auth headers were sent
	if capturedHeaders.Get("POLY_API_KEY") != testAPIKey {
		t.Errorf("POLY_API_KEY header = %q, want %q", capturedHeaders.Get("POLY_API_KEY"), testAPIKey)
	}
	if capturedHeaders.Get("POLY_SIGNATURE") == "" {
		t.Error("POLY_SIGNATURE header is empty")
	}
	if capturedHeaders.Get("POLY_TIMESTAMP") == "" {
		t.Error("POLY_TIMESTAMP header is empty")
	}

	// Verify request body has owner == API key
	var sentReq SendOrderRequest
	if err := json.Unmarshal(capturedBody, &sentReq); err != nil {
		t.Fatalf("unmarshal captured body: %v", err)
	}
	if sentReq.Owner != testAPIKey {
		t.Errorf("sent owner = %q, want %q", sentReq.Owner, testAPIKey)
	}
}

func TestPostOrder_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid order"}`))
	}))
	defer srv.Close()

	client, err := NewClient(testConfig(srv.URL))
	if err != nil {
		t.Fatal(err)
	}

	req, err := client.BuildOrderRequest("12345", "BUY", "0.57", "10", "GTC", "0", false)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.PostOrder(context.Background(), req)
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
}

// --- L2 Auth Headers ---

func TestL2Headers_Format(t *testing.T) {
	headers, err := L2Headers(testAPIKey, testSecret, "pass", testAddress, "POST", "/order", []byte(`{"test":true}`))
	if err != nil {
		t.Fatal(err)
	}

	if headers["POLY_API_KEY"] != testAPIKey {
		t.Errorf("POLY_API_KEY = %q", headers["POLY_API_KEY"])
	}
	if headers["POLY_ADDRESS"] != testAddress {
		t.Errorf("POLY_ADDRESS = %q", headers["POLY_ADDRESS"])
	}
	if headers["POLY_PASSPHRASE"] != "pass" {
		t.Errorf("POLY_PASSPHRASE = %q", headers["POLY_PASSPHRASE"])
	}
	if headers["POLY_SIGNATURE"] == "" {
		t.Error("POLY_SIGNATURE is empty")
	}
	if headers["POLY_TIMESTAMP"] == "" {
		t.Error("POLY_TIMESTAMP is empty")
	}
}

func TestL2Headers_Deterministic(t *testing.T) {
	body := []byte(`{"test":true}`)
	h1, err := l2HeadersWithTimestamp(testAPIKey, testSecret, "pass", testAddress, "POST", "/order", body, "1234567890")
	if err != nil {
		t.Fatal(err)
	}
	h2, err := l2HeadersWithTimestamp(testAPIKey, testSecret, "pass", testAddress, "POST", "/order", body, "1234567890")
	if err != nil {
		t.Fatal(err)
	}
	if h1["POLY_SIGNATURE"] != h2["POLY_SIGNATURE"] {
		t.Error("same inputs should produce same HMAC signature")
	}
}
