// Package controller implements reconciliation loops that operate as gRPC clients.
package controller

import (
	"context"
	"io"
	"log/slog"
	"time"

	pb "github.com/roysav/marketplane/api/gen"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const allocationType = "core/v1/Allocation"

// AllocationController watches for new Allocation records via the gRPC Watch
// stream and reconciles them by appending ledger entries and updating status.
// It also periodically resyncs to catch any missed events.
type AllocationController struct {
	records        pb.RecordServiceClient
	ledger         pb.LedgerServiceClient
	logger         *slog.Logger
	resyncInterval time.Duration
}

type AllocationControllerConfig struct {
	Records        pb.RecordServiceClient
	Ledger         pb.LedgerServiceClient
	Logger         *slog.Logger
	ResyncInterval time.Duration
}

func NewAllocationController(cfg AllocationControllerConfig) *AllocationController {
	return &AllocationController{
		records:        cfg.Records,
		ledger:         cfg.Ledger,
		logger:         cfg.Logger.With("component", "allocation-controller"),
		resyncInterval: cfg.ResyncInterval,
	}
}

// Run starts the watch loop and periodic resync. Blocks until ctx is canceled.
func (c *AllocationController) Run(ctx context.Context) error {
	// Initial resync on startup to catch anything created before the
	// controller was running.
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

// watchLoop opens a Watch stream and processes events. It also fires periodic
// resyncs via a ticker. Returns when the stream ends or ctx is canceled.
func (c *AllocationController) watchLoop(ctx context.Context) error {
	stream, err := c.records.Watch(ctx, &pb.WatchRequest{Type: allocationType})
	if err != nil {
		return err
	}

	ticker := time.NewTicker(c.resyncInterval)
	defer ticker.Stop()

	// Channel to receive stream events or errors from a goroutine, so we
	// can select on both the stream and the ticker.
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

func (c *AllocationController) handleWatchEvent(ctx context.Context, event *pb.WatchEvent) {
	if event.Action != "created" {
		return
	}
	rec := event.Record
	if rec == nil || rec.ObjectMeta == nil {
		return
	}
	c.reconcile(ctx, rec)
}

// resync lists all allocations and reconciles any that are not yet processed.
func (c *AllocationController) resync(ctx context.Context) {
	c.logger.Debug("starting resync")

	resp, err := c.records.List(ctx, &pb.ListRequest{Type: allocationType})
	if err != nil {
		c.logger.Error("resync: failed to list allocations", "error", err)
		return
	}

	reconciled := 0
	for _, rec := range resp.Records {
		if isReconciled(rec) {
			continue
		}
		c.reconcile(ctx, rec)
		reconciled++
	}

	c.logger.Debug("resync complete", "total", len(resp.Records), "reconciled", reconciled)
}

// reconcile processes a single allocation: appends a ledger entry and updates
// the allocation's status.
func (c *AllocationController) reconcile(ctx context.Context, rec *pb.Record) {
	meta := rec.ObjectMeta
	log := c.logger.With("tradespace", meta.Tradespace, "name", meta.Name)

	if isReconciled(rec) {
		return
	}

	spec := rec.Spec.AsMap()
	currency, _ := spec["currency"].(string)
	amount, _ := spec["amount"].(string)

	_, err := c.ledger.Append(ctx, &pb.AppendLedger{
		Allocation: meta.Name,
		Tradespace: meta.Tradespace,
		Amount:     amount,
		Currency:   currency,
	})
	if err != nil {
		st, _ := status.FromError(err)
		// AlreadyExists means the ledger entry was appended previously but
		// the status update failed — fall through to update status.
		if st.Code() != codes.AlreadyExists {
			log.Warn("reconcile: allocation rejected", "error", err)
			c.setStatus(ctx, rec, "rejected", err.Error())
			return
		}
	}

	c.setStatus(ctx, rec, "approved", "")
	log.Info("reconciled allocation", "amount", amount, "currency", currency)
}

func (c *AllocationController) setStatus(ctx context.Context, rec *pb.Record, phase, message string) {
	fields := map[string]any{"phase": phase}
	if message != "" {
		fields["message"] = message
	}

	st, err := structpb.NewStruct(fields)
	if err != nil {
		c.logger.Error("reconcile: failed to build status struct", "error", err)
		return
	}

	rec.Status = st

	_, err = c.records.Update(ctx, &pb.UpdateRequest{
		Record:      rec,
		LastApplied: rec.Spec,
	})
	if err != nil {
		c.logger.Error("reconcile: failed to update status",
			"tradespace", rec.ObjectMeta.Tradespace,
			"name", rec.ObjectMeta.Name,
			"error", err,
		)
	}
}

func isReconciled(rec *pb.Record) bool {
	if rec.Status == nil {
		return false
	}
	fields := rec.Status.AsMap()
	phase, _ := fields["phase"].(string)
	return phase == "approved"
}
