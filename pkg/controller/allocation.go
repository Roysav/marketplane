// Package controller provides controllers that reconcile record state.
package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/roysav/marketplane/api/gen"
)

const (
	AllocationTypeStr = "core/v1/Allocation"

	// Status phases
	PhasePending  = "Pending"
	PhaseApproved = "Approved"
	PhaseRejected = "Rejected"
)

// AllocationController reconciles Allocation records by appending to the ledger.
// It runs as a separate process and communicates with the API server via gRPC.
type AllocationController struct {
	records pb.RecordServiceClient
	ledger  pb.LedgerServiceClient
	logger  *slog.Logger
}

// NewAllocationController creates a new AllocationController.
func NewAllocationController(records pb.RecordServiceClient, ledger pb.LedgerServiceClient, logger *slog.Logger) *AllocationController {
	if logger == nil {
		logger = slog.Default()
	}
	return &AllocationController{
		records: records,
		ledger:  ledger,
		logger:  logger.With("component", "allocation-controller"),
	}
}

// Run starts the controller loop, watching for Allocation events.
func (c *AllocationController) Run(ctx context.Context) error {
	stream, err := c.records.Watch(ctx, &pb.WatchRequest{
		Type: AllocationTypeStr,
	})
	if err != nil {
		return err
	}

	c.logger.Info("allocation controller started")

	for {
		event, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				c.logger.Info("allocation controller stopped")
				return nil
			}
			return err
		}

		// Only process create events
		if event.Action != "created" {
			continue
		}

		if event.Record == nil {
			continue
		}

		if err := c.reconcile(ctx, event.Record); err != nil {
			c.logger.Error("failed to reconcile allocation",
				"tradespace", event.Record.ObjectMeta.GetTradespace(),
				"name", event.Record.ObjectMeta.GetName(),
				"error", err,
			)
		}
	}
}

func (c *AllocationController) reconcile(ctx context.Context, rec *pb.Record) error {
	tradespace := rec.ObjectMeta.GetTradespace()
	name := rec.ObjectMeta.GetName()

	// Check if already processed
	if rec.Status != nil {
		if phase, ok := rec.Status.Fields["phase"]; ok {
			phaseStr := phase.GetStringValue()
			if phaseStr == PhaseApproved || phaseStr == PhaseRejected {
				return nil
			}
		}
	}

	// Extract spec fields
	spec := rec.Spec.AsMap()
	currency, _ := spec["currency"].(string)
	amount, _ := spec["amount"].(string)
	targetType, _ := spec["targetType"].(string)
	targetName, _ := spec["targetName"].(string)

	if currency == "" || amount == "" || targetType == "" || targetName == "" {
		return c.rejectAllocation(ctx, rec, "missing required fields in spec")
	}

	// Attempt to append to ledger
	resp, err := c.ledger.Append(ctx, &pb.LedgerAppendRequest{
		Tradespace:     tradespace,
		Currency:       currency,
		Amount:         amount,
		AllocationName: name,
		TargetType:     targetType,
		TargetName:     targetName,
	})
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			switch st.Code() {
			case codes.FailedPrecondition:
				return c.rejectAllocation(ctx, rec, "insufficient balance")
			case codes.AlreadyExists:
				return c.rejectAllocation(ctx, rec, "target already has an allocation")
			}
		}
		return err
	}

	// Approve the allocation
	return c.approveAllocation(ctx, rec, resp.Entry.GetId())
}

func (c *AllocationController) approveAllocation(ctx context.Context, rec *pb.Record, ledgerID string) error {
	statusMap := map[string]any{
		"phase":         PhaseApproved,
		"ledgerEntryID": ledgerID,
		"message":       "allocation approved and recorded in ledger",
	}
	statusStruct, err := structpb.NewStruct(statusMap)
	if err != nil {
		return err
	}

	rec.Status = statusStruct
	_, err = c.records.Update(ctx, &pb.UpdateRequest{Record: rec})
	if err != nil {
		return err
	}

	c.logger.Info("allocation approved",
		"tradespace", rec.ObjectMeta.GetTradespace(),
		"name", rec.ObjectMeta.GetName(),
		"ledgerID", ledgerID,
	)
	return nil
}

func (c *AllocationController) rejectAllocation(ctx context.Context, rec *pb.Record, reason string) error {
	statusMap := map[string]any{
		"phase":   PhaseRejected,
		"message": reason,
	}
	statusStruct, err := structpb.NewStruct(statusMap)
	if err != nil {
		return err
	}

	rec.Status = statusStruct
	_, err = c.records.Update(ctx, &pb.UpdateRequest{Record: rec})
	if err != nil {
		return err
	}

	c.logger.Warn("allocation rejected",
		"tradespace", rec.ObjectMeta.GetTradespace(),
		"name", rec.ObjectMeta.GetName(),
		"reason", reason,
	)
	return nil
}
