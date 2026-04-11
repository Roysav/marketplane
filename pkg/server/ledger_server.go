package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/authz"
	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/storage"
)

// LedgerServer implements the gRPC LedgerService.
type LedgerServer struct {
	pb.UnimplementedLedgerServiceServer
	ledger     storage.LedgerStorage
	rows       storage.RowStorage
	authorizer *authz.Authorizer
	logger     *slog.Logger
}

// NewLedgerServer creates a new gRPC ledger server.
func NewLedgerServer(ledger storage.LedgerStorage, rows storage.RowStorage, authorizer *authz.Authorizer, logger *slog.Logger) *LedgerServer {
	if logger == nil {
		logger = slog.Default()
	}
	if authorizer == nil {
		authorizer = authz.New(authz.Config{})
	}
	return &LedgerServer{
		ledger:     ledger,
		rows:       rows,
		authorizer: authorizer,
		logger:     logger.With("component", "grpc-ledger"),
	}
}

// Register registers the server with a gRPC server.
func (s *LedgerServer) Register(grpcServer *grpc.Server) {
	pb.RegisterLedgerServiceServer(grpcServer, s)
}

// Append atomically checks balance and inserts a ledger entry.
func (s *LedgerServer) Append(ctx context.Context, req *pb.LedgerAppendRequest) (*pb.LedgerAppendResponse, error) {
	if req.Tradespace == "" {
		return nil, status.Error(codes.InvalidArgument, "tradespace is required")
	}
	if req.AllocationName == "" {
		return nil, status.Error(codes.InvalidArgument, "allocation_name is required")
	}
	if err := s.authorizer.AuthorizeLedger(ctx, authz.VerbAppend, req.Tradespace); err != nil {
		return nil, toGRPCError(err)
	}

	allocation, err := s.loadAllocation(ctx, req.Tradespace, req.AllocationName)
	if err != nil {
		return nil, err
	}
	if err := validateLedgerAppendMatchesAllocation(req, allocation); err != nil {
		return nil, err
	}

	entry := &storage.LedgerEntry{
		Tradespace:     req.Tradespace,
		Currency:       req.Currency,
		Amount:         req.Amount,
		AllocationName: req.AllocationName,
		TargetType:     req.TargetType,
		TargetName:     req.TargetName,
	}

	if err := s.ledger.Append(ctx, entry); err != nil {
		if errors.Is(err, storage.ErrInsufficientBalance) {
			return nil, status.Error(codes.FailedPrecondition, "insufficient balance")
		}
		if errors.Is(err, storage.ErrAlreadyAllocated) {
			return nil, status.Error(codes.AlreadyExists, "target already has an allocation")
		}
		if errors.Is(err, storage.ErrAllocationApplied) {
			return nil, status.Error(codes.AlreadyExists, "allocation already has a ledger entry")
		}
		s.logger.Error("failed to append ledger entry", "error", err)
		return nil, status.Error(codes.Internal, "failed to append ledger entry")
	}

	s.logger.Debug("ledger entry appended",
		"tradespace", entry.Tradespace,
		"currency", entry.Currency,
		"amount", entry.Amount,
		"target", entry.TargetType+"/"+entry.TargetName,
	)

	return &pb.LedgerAppendResponse{
		Entry: storageEntryToPB(entry),
	}, nil
}

// GetByAllocation returns the ledger entry for an Allocation.
func (s *LedgerServer) GetByAllocation(ctx context.Context, req *pb.LedgerGetByAllocationRequest) (*pb.LedgerGetByAllocationResponse, error) {
	if req.Tradespace == "" {
		return nil, status.Error(codes.InvalidArgument, "tradespace is required")
	}
	if req.AllocationName == "" {
		return nil, status.Error(codes.InvalidArgument, "allocation_name is required")
	}
	if err := s.authorizer.AuthorizeLedger(ctx, authz.VerbGet, req.Tradespace); err != nil {
		return nil, toGRPCError(err)
	}

	entry, err := s.ledger.GetByAllocation(ctx, req.Tradespace, req.AllocationName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "ledger entry not found")
		}
		s.logger.Error("failed to get ledger entry by allocation", "error", err)
		return nil, status.Error(codes.Internal, "failed to get ledger entry")
	}

	return &pb.LedgerGetByAllocationResponse{
		Entry: storageEntryToPB(entry),
	}, nil
}

// List returns all ledger entries for a tradespace.
func (s *LedgerServer) List(ctx context.Context, req *pb.LedgerListRequest) (*pb.LedgerListResponse, error) {
	if req.Tradespace == "" {
		return nil, status.Error(codes.InvalidArgument, "tradespace is required")
	}
	if err := s.authorizer.AuthorizeLedger(ctx, authz.VerbList, req.Tradespace); err != nil {
		return nil, toGRPCError(err)
	}

	entries, err := s.ledger.List(ctx, req.Tradespace)
	if err != nil {
		s.logger.Error("failed to list ledger entries", "error", err)
		return nil, status.Error(codes.Internal, "failed to list ledger entries")
	}

	pbEntries := make([]*pb.LedgerEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = storageEntryToPB(e)
	}

	return &pb.LedgerListResponse{
		Entries: pbEntries,
	}, nil
}

func (s *LedgerServer) loadAllocation(ctx context.Context, tradespace, allocationName string) (*pb.Record, error) {
	row, err := s.rows.Get(ctx, storage.Key{
		Type:       "core/v1/Allocation",
		Tradespace: tradespace,
		Name:       allocationName,
	})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "allocation not found")
		}
		s.logger.Error("failed to load allocation", "error", err)
		return nil, status.Error(codes.Internal, "failed to load allocation")
	}

	rec, err := storageRowToRecord(row)
	if err != nil {
		s.logger.Error("failed to decode allocation", "error", err)
		return nil, status.Error(codes.Internal, "failed to decode allocation")
	}
	pbRec, err := recordToPB(rec)
	if err != nil {
		s.logger.Error("failed to convert allocation", "error", err)
		return nil, status.Error(codes.Internal, "failed to convert allocation")
	}
	return pbRec, nil
}

func validateLedgerAppendMatchesAllocation(req *pb.LedgerAppendRequest, allocation *pb.Record) error {
	if allocation.Spec == nil {
		return status.Error(codes.InvalidArgument, "allocation spec is required")
	}
	spec := allocation.Spec.AsMap()
	if currency, _ := spec["currency"].(string); currency != req.Currency {
		return status.Error(codes.InvalidArgument, "currency must match allocation")
	}
	if amount, _ := spec["amount"].(string); amount != req.Amount {
		return status.Error(codes.InvalidArgument, "amount must match allocation")
	}
	if targetType, _ := spec["targetType"].(string); targetType != req.TargetType {
		return status.Error(codes.InvalidArgument, "target_type must match allocation")
	}
	if targetName, _ := spec["targetName"].(string); targetName != req.TargetName {
		return status.Error(codes.InvalidArgument, "target_name must match allocation")
	}
	return nil
}

func storageEntryToPB(e *storage.LedgerEntry) *pb.LedgerEntry {
	return &pb.LedgerEntry{
		Id:             e.ID,
		Tradespace:     e.Tradespace,
		Currency:       e.Currency,
		Amount:         e.Amount,
		AllocationName: e.AllocationName,
		TargetType:     e.TargetType,
		TargetName:     e.TargetName,
		CreatedAt:      timestamppb.New(e.CreatedAt),
	}
}

func storageRowToRecord(row *storage.Row) (*record.Record, error) {
	var data struct {
		Spec   map[string]any `json:"spec"`
		Status map[string]any `json:"status"`
	}
	if row.Data != "" {
		if err := json.Unmarshal([]byte(row.Data), &data); err != nil {
			return nil, fmt.Errorf("failed to unmarshal record data: %w", err)
		}
	}

	return &record.Record{
		TypeMeta: record.TypeMetaFromType(row.Type),
		ObjectMeta: record.ObjectMeta{
			Tradespace:      row.Tradespace,
			Name:            row.Name,
			Labels:          row.Labels,
			ResourceVersion: row.ResourceVersion,
			CreatedAt:       row.CreatedAt,
			UpdatedAt:       row.UpdatedAt,
		},
		Spec:   data.Spec,
		Status: data.Status,
	}, nil
}
