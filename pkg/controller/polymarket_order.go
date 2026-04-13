package controller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/polymarket"
	"google.golang.org/protobuf/types/known/structpb"
)

const polymarketOrderType = "polymarket/v1/Order"

// PolymarketOrderController watches polymarket/v1/Order records and submits
// them to the Polymarket CLOB exactly once.
type PolymarketOrderController struct {
	records        pb.RecordServiceClient
	poly           *polymarket.Client
	logger         *slog.Logger
	resyncInterval time.Duration
}

type PolymarketOrderControllerConfig struct {
	Records        pb.RecordServiceClient
	Poly           *polymarket.Client
	Logger         *slog.Logger
	ResyncInterval time.Duration
}

func NewPolymarketOrderController(cfg PolymarketOrderControllerConfig) *PolymarketOrderController {
	return &PolymarketOrderController{
		records:        cfg.Records,
		poly:           cfg.Poly,
		logger:         cfg.Logger.With("component", "polymarket-order-controller"),
		resyncInterval: cfg.ResyncInterval,
	}
}

// Run starts the watch loop and periodic resync. Blocks until ctx is canceled.
func (c *PolymarketOrderController) Run(ctx context.Context) error {
	c.resync(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := c.watchLoop(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.logger.Error("watch stream failed, will reconnect", "error", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func (c *PolymarketOrderController) watchLoop(ctx context.Context) error {
	stream, err := c.records.Watch(ctx, &pb.WatchRequest{Type: polymarketOrderType})
	if err != nil {
		return err
	}

	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()

	type watchResult struct {
		event *pb.WatchEvent
		err   error
	}
	ch := make(chan watchResult, 1)

	go func() {
		for {
			event, err := stream.Recv()
			ch <- watchResult{event, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-ch:
			if res.err != nil {
				if res.err == io.EOF {
					return nil
				}
				return res.err
			}
			c.handleWatchEvent(ctx, res.event)
		case <-ticker.C:
			c.resync(ctx)
		}
	}
}

func (c *PolymarketOrderController) handleWatchEvent(ctx context.Context, event *pb.WatchEvent) {
	rec := event.Record
	if rec == nil || rec.ObjectMeta == nil {
		return
	}
	c.reconcile(ctx, rec)
}

func (c *PolymarketOrderController) resync(ctx context.Context) {
	c.logger.Debug("starting resync")

	resp, err := c.records.List(ctx, &pb.ListRequest{Type: polymarketOrderType})
	if err != nil {
		c.logger.Error("resync: failed to list orders", "error", err)
		return
	}

	reconciled := 0
	for _, rec := range resp.Records {
		if c.isReconciled(rec) {
			continue
		}
		c.reconcile(ctx, rec)
		reconciled++
	}

	c.logger.Debug("resync complete", "total", len(resp.Records), "reconciled", reconciled)
}

func (c *PolymarketOrderController) reconcile(ctx context.Context, rec *pb.Record) {
	meta := rec.ObjectMeta
	log := c.logger.With("tradespace", meta.Tradespace, "name", meta.Name)

	if c.isReconciled(rec) {
		return
	}

	spec := rec.Spec.AsMap()
	active, _ := spec["active"].(bool)

	// Draft gate: active=false → set Draft status, do nothing else.
	if !active {
		c.setStatus(ctx, rec, map[string]any{
			"state":   "Draft",
			"message": "spec.active is false",
		})
		return
	}

	// Active and no polymarketOrderID — sign and submit exactly once.
	tokenID, _ := spec["tokenId"].(string)
	side, _ := spec["side"].(string)
	price, _ := spec["price"].(string)
	size, _ := spec["size"].(string)
	orderType, _ := spec["orderType"].(string)
	if orderType == "" {
		orderType = "GTC"
	}
	expiration, _ := spec["expiration"].(string)
	if expiration == "" {
		expiration = "0"
	}
	deferExec, _ := spec["deferExec"].(bool)

	orderReq, err := c.poly.BuildOrderRequest(tokenID, side, price, size, orderType, expiration, deferExec)
	if err != nil {
		log.Error("failed to build order request", "error", err)
		c.setStatus(ctx, rec, map[string]any{
			"state":   "Failed",
			"message": fmt.Sprintf("build order: %v", err),
		})
		return
	}

	resp, err := c.poly.PostOrder(ctx, orderReq)
	if err != nil {
		log.Error("failed to post order", "error", err)
		c.setStatus(ctx, rec, map[string]any{
			"state":   "Failed",
			"message": fmt.Sprintf("POST /order: %v", err),
		})
		return
	}

	// Build external status from raw response.
	external := map[string]any{
		"orderID":     resp.OrderID,
		"orderStatus": resp.Status,
	}
	if resp.MakingAmount != "" {
		external["makingAmount"] = resp.MakingAmount
	}
	if resp.TakingAmount != "" {
		external["takingAmount"] = resp.TakingAmount
	}
	if len(resp.TransactionsHashes) > 0 {
		hashes := make([]any, len(resp.TransactionsHashes))
		for i, h := range resp.TransactionsHashes {
			hashes[i] = h
		}
		external["transactionHashes"] = hashes
	}
	if len(resp.TradeIDs) > 0 {
		ids := make([]any, len(resp.TradeIDs))
		for i, id := range resp.TradeIDs {
			ids[i] = id
		}
		external["tradeIDs"] = ids
	}
	if resp.ErrorMsg != "" {
		external["errorMsg"] = resp.ErrorMsg
	}

	state := "Live"
	message := ""
	if !resp.Success {
		state = "Failed"
		message = resp.ErrorMsg
		if message == "" {
			message = "POST /order returned success=false"
		}
		// Don't set polymarketOrderID on failure — allows retry after user
		// resets active=false→true.
		c.setStatus(ctx, rec, map[string]any{
			"state":    state,
			"message":  message,
			"external": external,
		})
		return
	}

	switch resp.Status {
	case "matched":
		state = "Matched"
	case "delayed":
		state = "Delayed"
	}

	c.setStatus(ctx, rec, map[string]any{
		"state":             state,
		"message":           message,
		"polymarketOrderID": resp.OrderID,
		"external":          external,
	})
	log.Info("order submitted", "orderID", resp.OrderID, "state", state)
}

func (c *PolymarketOrderController) setStatus(ctx context.Context, rec *pb.Record, statusFields map[string]any) {
	st, err := structpb.NewStruct(statusFields)
	if err != nil {
		c.logger.Error("failed to build status struct", "error", err)
		return
	}

	rec.Status = st

	_, err = c.records.Update(ctx, &pb.UpdateRequest{
		Record:      rec,
		LastApplied: rec.Spec,
	})
	if err != nil {
		c.logger.Error("failed to update status",
			"tradespace", rec.ObjectMeta.Tradespace,
			"name", rec.ObjectMeta.Name,
			"error", err,
		)
	}
}

func (c *PolymarketOrderController) isReconciled(rec *pb.Record) bool {
	if rec.Status == nil {
		return false
	}
	fields := rec.Status.AsMap()

	// Already submitted successfully — polymarketOrderID is the key signal.
	if id, _ := fields["polymarketOrderID"].(string); id != "" {
		return true
	}

	state, _ := fields["state"].(string)

	var active bool
	if rec.Spec != nil {
		spec := rec.Spec.AsMap()
		active, _ = spec["active"].(bool)
	}

	// Draft and inactive — nothing to do.
	if !active && state == "Draft" {
		return true
	}

	// Failed/Rejected while still active — user hasn't intervened.
	// Once user sets active=false (→ Draft) then active=true again, retry is possible.
	if (state == "Failed" || state == "Rejected") && active {
		return true
	}

	return false
}
