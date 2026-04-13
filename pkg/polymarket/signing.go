package polymarket

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	dcrdecdsa "github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"golang.org/x/crypto/sha3"
)

const (
	NegRiskExchange = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
	CTFExchange     = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
	ZeroAddress     = "0x0000000000000000000000000000000000000000"
)

var (
	domainTypeHash = keccak256([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	orderTypeHash  = keccak256([]byte("Order(uint256 salt,address maker,address signer,address taker,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint256 expiration,uint256 nonce,uint256 feeRateBps,uint8 side,uint8 signatureType)"))
	domainName     = keccak256([]byte("Polymarket CTF Exchange"))
	domainVersion  = keccak256([]byte("1"))
)

func keccak256(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

func buildDomainSeparator(chainID int64, exchangeAddress string) []byte {
	data := make([]byte, 0, 5*32)
	data = append(data, domainTypeHash...)
	data = append(data, domainName...)
	data = append(data, domainVersion...)
	data = append(data, uint256Bytes(big.NewInt(chainID))...)
	data = append(data, addressToBytes32(exchangeAddress)...)
	return keccak256(data)
}

func buildOrderStructHash(order *OrderPayload) ([]byte, error) {
	salt, ok := new(big.Int).SetString(string(order.Salt), 10)
	if !ok {
		return nil, fmt.Errorf("invalid salt: %s", order.Salt)
	}
	tokenID, ok := new(big.Int).SetString(order.TokenID, 10)
	if !ok {
		return nil, fmt.Errorf("invalid tokenId: %s", order.TokenID)
	}
	makerAmount, ok := new(big.Int).SetString(order.MakerAmount, 10)
	if !ok {
		return nil, fmt.Errorf("invalid makerAmount: %s", order.MakerAmount)
	}
	takerAmount, ok := new(big.Int).SetString(order.TakerAmount, 10)
	if !ok {
		return nil, fmt.Errorf("invalid takerAmount: %s", order.TakerAmount)
	}
	expiration, ok := new(big.Int).SetString(order.Expiration, 10)
	if !ok {
		return nil, fmt.Errorf("invalid expiration: %s", order.Expiration)
	}
	nonce, ok := new(big.Int).SetString(order.Nonce, 10)
	if !ok {
		return nil, fmt.Errorf("invalid nonce: %s", order.Nonce)
	}
	feeRateBps, ok := new(big.Int).SetString(order.FeeRateBps, 10)
	if !ok {
		return nil, fmt.Errorf("invalid feeRateBps: %s", order.FeeRateBps)
	}

	// side: BUY=0, SELL=1
	side := big.NewInt(0)
	if order.Side == "SELL" {
		side = big.NewInt(1)
	}

	data := make([]byte, 0, 14*32)
	data = append(data, orderTypeHash...)
	data = append(data, uint256Bytes(salt)...)
	data = append(data, addressToBytes32(order.Maker)...)
	data = append(data, addressToBytes32(order.Signer)...)
	data = append(data, addressToBytes32(order.Taker)...)
	data = append(data, uint256Bytes(tokenID)...)
	data = append(data, uint256Bytes(makerAmount)...)
	data = append(data, uint256Bytes(takerAmount)...)
	data = append(data, uint256Bytes(expiration)...)
	data = append(data, uint256Bytes(nonce)...)
	data = append(data, uint256Bytes(feeRateBps)...)
	data = append(data, uint256Bytes(side)...)
	data = append(data, uint256Bytes(big.NewInt(int64(order.SignatureType)))...)
	return keccak256(data), nil
}

func signOrder(domainSeparator []byte, structHash []byte, privKey *secp256k1.PrivateKey) ([]byte, error) {
	// EIP-712: "\x19\x01" ++ domainSeparator ++ structHash
	msg := make([]byte, 0, 2+32+32)
	msg = append(msg, 0x19, 0x01)
	msg = append(msg, domainSeparator...)
	msg = append(msg, structHash...)
	hash := keccak256(msg)

	// SignCompact returns [v (1 byte, 27|28), r (32 bytes), s (32 bytes)]
	sig := dcrdecdsa.SignCompact(privKey, hash, false)

	// Rearrange to Ethereum format: [r (32), s (32), v (1)]
	ethSig := make([]byte, 65)
	copy(ethSig[0:32], sig[1:33])
	copy(ethSig[32:64], sig[33:65])
	ethSig[64] = sig[0] // v: 27 or 28
	return ethSig, nil
}

// computeAmounts calculates maker and taker amounts from price and size.
// Returns 6-decimal fixed-point integer strings matching py-clob-client logic:
//
//	BUY:  makerAmount = round(rawSize * rawPrice / 1e6), takerAmount = rawSize
//	SELL: makerAmount = rawSize, takerAmount = round(rawSize * rawPrice / 1e6)
func computeAmounts(side, priceStr, sizeStr string) (makerAmt, takerAmt string, err error) {
	rawPrice, err := decimalToFixed(priceStr, 6)
	if err != nil {
		return "", "", fmt.Errorf("invalid price: %w", err)
	}
	rawSize, err := decimalToFixed(sizeStr, 6)
	if err != nil {
		return "", "", fmt.Errorf("invalid size: %w", err)
	}

	// notional = rawSize * rawPrice / 1e6 (rounded)
	product := new(big.Int).Mul(rawSize, rawPrice)
	scale := big.NewInt(1_000_000)
	notional := new(big.Int).Div(product, scale)
	remainder := new(big.Int).Mod(product, scale)
	doubled := new(big.Int).Mul(remainder, big.NewInt(2))
	if doubled.Cmp(scale) >= 0 {
		notional.Add(notional, big.NewInt(1))
	}

	if side == "BUY" {
		return notional.String(), rawSize.String(), nil
	}
	return rawSize.String(), notional.String(), nil
}

// decimalToFixed converts a decimal string to a fixed-point integer with given decimals.
func decimalToFixed(s string, decimals int) (*big.Int, error) {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("invalid decimal: %s", s)
	}

	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	r.Mul(r, new(big.Rat).SetInt(scale))

	num := r.Num()
	den := r.Denom()
	result := new(big.Int).Div(num, den)
	rem := new(big.Int).Mod(num, den)
	doubled := new(big.Int).Mul(rem, big.NewInt(2))
	if doubled.Cmp(den) >= 0 {
		result.Add(result, big.NewInt(1))
	}
	return result, nil
}

func randomSalt() json.Number {
	max := new(big.Int).Exp(big.NewInt(2), big.NewInt(128), nil)
	salt, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(fmt.Sprintf("failed to generate random salt: %v", err))
	}
	return json.Number(salt.String())
}

func parsePrivateKey(hexKey string) (*secp256k1.PrivateKey, error) {
	hexKey = strings.TrimPrefix(hexKey, "0x")
	keyBytes, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("invalid private key hex: %w", err)
	}
	return secp256k1.PrivKeyFromBytes(keyBytes), nil
}

func uint256Bytes(n *big.Int) []byte {
	b := make([]byte, 32)
	if n != nil {
		nb := n.Bytes()
		copy(b[32-len(nb):], nb)
	}
	return b
}

func addressToBytes32(addr string) []byte {
	addr = strings.TrimPrefix(strings.ToLower(addr), "0x")
	addrBytes, _ := hex.DecodeString(addr)
	b := make([]byte, 32)
	copy(b[32-len(addrBytes):], addrBytes)
	return b
}
