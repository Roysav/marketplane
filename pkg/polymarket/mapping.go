package polymarket

import (
	"fmt"
	"strconv"
	"time"

	"github.com/GoPolymarket/polymarket-go-sdk/pkg/clob/clobtypes"
	"github.com/GoPolymarket/polymarket-go-sdk/pkg/types"
	"github.com/ethereum/go-ethereum/common"
)

func ethAddress(s string) types.Address {
	if s == "" {
		return types.Address{}
	}
	return types.Address(common.HexToAddress(s))
}

// unixSecsToTime maps Polymarket's "0 means absent" unix-seconds convention to
// a Go time.Time. Zero stays zero; anything else becomes a UTC instant.
func unixSecsToTime(secs int64) time.Time {
	if secs <= 0 {
		return time.Time{}
	}
	return time.Unix(secs, 0).UTC()
}

func mapOrderResponse(r clobtypes.OrderResponse) *OrderResponse {
	return &OrderResponse{
		OrderID: r.ID,
		Status:  r.Status,
		Raw: map[string]any{
			"orderID":       r.ID,
			"status":        r.Status,
			"asset_id":      r.AssetID,
			"market":        r.Market,
			"side":          r.Side,
			"price":         r.Price,
			"original_size": r.OriginalSize,
			"size_matched":  r.SizeMatched,
			"owner":         r.Owner,
			"maker_address": r.MakerAddress,
			"order_type":    r.OrderType,
			"expiration":    r.Expiration,
			"created_at":    r.CreatedAt,
			"timestamp":     r.Timestamp,
			"outcome":       r.Outcome,
		},
	}
}

func mapRemoteOrder(r clobtypes.OrderResponse) (*RemoteOrder, error) {
	expiration, err := strconv.ParseInt(r.Expiration, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("remote order %s: parse expiration %q: %w", r.ID, r.Expiration, err)
	}

	createdSecs, err := strconv.ParseInt(r.CreatedAt, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("remote order %s: parse created_at %q: %w", r.ID, r.CreatedAt, err)
	}
	created := unixSecsToTime(createdSecs)

	return &RemoteOrder{
		ID:           r.ID,
		Status:       r.Status,
		Owner:        r.Owner,
		MakerAddress: r.MakerAddress,
		Market:       r.Market,
		AssetID:      r.AssetID,
		Side:         Side(r.Side),
		OriginalSize: r.OriginalSize,
		SizeMatched:  r.SizeMatched,
		Price:        r.Price,
		Outcome:      r.Outcome,
		OrderType:    OrderType(r.OrderType),
		Expiration:   expiration,
		CreatedAt:    created,
		Raw: map[string]any{
			"orderID":       r.ID,
			"status":        r.Status,
			"asset_id":      r.AssetID,
			"market":        r.Market,
			"side":          r.Side,
			"price":         r.Price,
			"original_size": r.OriginalSize,
			"size_matched":  r.SizeMatched,
			"owner":         r.Owner,
			"maker_address": r.MakerAddress,
			"order_type":    r.OrderType,
			"expiration":    r.Expiration,
			"created_at":    r.CreatedAt,
			"outcome":       r.Outcome,
		},
	}, nil
}

func mapRemoteTrade(t clobtypes.Trade) *RemoteTrade {
	orderID := t.TakerOrderID
	if orderID == "" {
		orderID = t.MakerOrderID
	}
	return &RemoteTrade{
		TradeID:         t.ID,
		OrderID:         orderID,
		Market:          t.Market,
		AssetID:         t.AssetID,
		Side:            Side(t.Side),
		Price:           t.Price,
		Size:            t.Size,
		MakerAddress:    t.MakerAddress,
		Status:          TradeStatus(t.Status),
		TransactionHash: t.TransactionHash,
		MatchTime:       unixSecsToTime(t.Timestamp),
		Raw: map[string]any{
			"id":               t.ID,
			"price":            t.Price,
			"size":             t.Size,
			"side":             t.Side,
			"timestamp":        t.Timestamp,
			"market":           t.Market,
			"asset_id":         t.AssetID,
			"status":           t.Status,
			"taker_order_id":   t.TakerOrderID,
			"maker_order_id":   t.MakerOrderID,
			"owner":            t.Owner,
			"maker_address":    t.MakerAddress,
			"match_time":       t.MatchTime,
			"fee_rate_bps":     t.FeeRateBps,
			"transaction_hash": t.TransactionHash,
		},
	}
}
