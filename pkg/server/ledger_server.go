package server

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/storage"
)

// LedgerServer implements the gRPC LedgerService.
type LedgerServer struct {
	pb.UnimplementedLedgerServiceServer
	ledger storage.LedgerStorage
	logger *slog.Logger
}

// NewLedgerServer creates a new gRPC ledger server.
func NewLedgerServer(ledger storage.LedgerStorage, logger *slog.Logger) *LedgerServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &LedgerServer{
		ledger: ledger,
		logger: logger.With("component", "grpc-ledger"),
	}
}

// Register registers the server with a gRPC server.
func (s *LedgerServer) Register(grpcServer *grpc.Server) {
	pb.RegisterLedgerServiceServer(grpcServer, s)
}

// Append atomically checks balance and inserts a ledger entry.
func (s *LedgerServer) Append(ctx context.Context, req *pb.LedgerAppendRequest) (*pb.LedgerAppendResponse, error) {
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

// List returns all ledger entries for a tradespace.
func (s *LedgerServer) List(ctx context.Context, req *pb.LedgerListRequest) (*pb.LedgerListResponse, error) {
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
