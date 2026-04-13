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
	return &pb.AppendLedgerResponse{}, ledgerToStreamGRPCError(s.svc.Append(ctx, in.Allocation, in.Tradespace, in.Amount, in.Currency))
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

func ledgerToStreamGRPCError(err error) error {
	if err == nil {
		return nil
	}

	return status.Error(codes.Internal, err.Error())
}
