package server

import (
	"context"
	"log/slog"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LedgerServer struct {
	pb.UnimplementedLedgerServiceServer
	svc    *service.LedgerService
	logger *slog.Logger
}

func (s *LedgerServer) Append(ctx context.Context, in *pb.AppendLedger) (*pb.AppendLedgerResponse, error) {
	return &pb.AppendLedgerResponse{}, ledgerToGRPCError(s.svc.Append(ctx, in.Allocation, in.Tradespace, in.Amount, in.Currency))
}

func (s *LedgerServer) List(ctx context.Context, in *pb.ListLedgerRequest) (*pb.ListLedgerResponse, error) {
	entries, err := s.svc.List(ctx, in.Tradespace)
	if err != nil {
		return nil, ledgerToGRPCError(err)
	}

	pbEntries := make([]*pb.LedgerEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.LedgerEntry{
			Key:       e.Key,
			Amount:    e.Amount,
			Remaining: e.Remaining,
			Currency:  e.Currency,
		}
	}

	return &pb.ListLedgerResponse{Entries: pbEntries}, nil
}

// NewLedgerServer creates a new gRPC server.
func NewLedgerServer(svc *service.LedgerService, logger *slog.Logger) *LedgerServer {
	return &LedgerServer{
		svc:    svc,
		logger: logger.With("component", "grpc"),
	}
}

func (s *LedgerServer) Register(grpcServer *grpc.Server) {
	pb.RegisterLedgerServiceServer(grpcServer, s)
}

func ledgerToGRPCError(err error) error {
	if err == nil {
		return nil
	}

	return status.Error(codes.Internal, err.Error())
}
