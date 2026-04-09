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
	"github.com/roysav/marketplane/pkg/service"
)

// StreamServer implements the gRPC StreamService.
type StreamServer struct {
	pb.UnimplementedStreamServiceServer
	svc    *service.StreamService
	logger *slog.Logger
}

// NewStreamServer creates a new gRPC stream server.
func NewStreamServer(svc *service.StreamService, logger *slog.Logger) *StreamServer {
	if logger == nil {
		logger = slog.Default()
	}
	return &StreamServer{
		svc:    svc,
		logger: logger.With("component", "grpc-stream"),
	}
}

// Register registers the server with a gRPC server.
func (s *StreamServer) Register(grpcServer *grpc.Server) {
	pb.RegisterStreamServiceServer(grpcServer, s)
}

// Append adds a data point to a stream.
func (s *StreamServer) Append(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	key, err := validateStreamKey(req.Key)
	if err != nil {
		return nil, err
	}
	if req.Timestamp == nil {
		return nil, status.Error(codes.InvalidArgument, "timestamp is required")
	}
	if req.Data == nil {
		return nil, status.Error(codes.InvalidArgument, "data is required")
	}

	ts := req.Timestamp.AsTime()
	data := req.Data.AsMap()

	err = s.svc.Append(ctx, key, ts, data)
	if err != nil {
		return nil, toStreamGRPCError(err)
	}

	return &pb.AppendResponse{}, nil
}

// Latest gets the most recent entry from a stream.
func (s *StreamServer) Latest(ctx context.Context, req *pb.LatestRequest) (*pb.LatestResponse, error) {
	key, err := validateStreamKey(req.Key)
	if err != nil {
		return nil, err
	}

	entry, err := s.svc.Latest(ctx, key)
	if err != nil {
		return nil, toStreamGRPCError(err)
	}

	return &pb.LatestResponse{
		Entry: &pb.StreamEntry{
			Key:       pbStreamKey(key),
			Timestamp: timestamppb.New(entry.Timestamp),
			Value:     entry.Value,
		},
	}, nil
}

// Range gets entries within a time range.
func (s *StreamServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	key, err := validateStreamKey(req.Key)
	if err != nil {
		return nil, err
	}
	if req.From == nil || req.To == nil {
		return nil, status.Error(codes.InvalidArgument, "from and to timestamps are required")
	}

	entries, err := s.svc.Range(ctx, key, req.From.AsTime(), req.To.AsTime())
	if err != nil {
		return nil, toStreamGRPCError(err)
	}

	pbEntries := make([]*pb.StreamEntry, len(entries))
	for i, e := range entries {
		pbEntries[i] = &pb.StreamEntry{
			Key:       pbStreamKey(key),
			Timestamp: timestamppb.New(e.Timestamp),
			Value:     e.Value,
		}
	}

	return &pb.RangeResponse{Entries: pbEntries}, nil
}

// Watch streams new entries as they arrive.
func (s *StreamServer) Watch(req *pb.StreamWatchRequest, stream grpc.ServerStreamingServer[pb.StreamEntry]) error {
	key, err := validateStreamKey(req.Key)
	if err != nil {
		return err
	}

	ctx := stream.Context()

	eventCh, err := s.svc.Watch(ctx, key)
	if err != nil {
		return toStreamGRPCError(err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-eventCh:
			if !ok {
				return nil
			}

			if err := stream.Send(&pb.StreamEntry{
				Key:       pbStreamKey(key),
				Timestamp: timestamppb.New(event.Entry.Timestamp),
				Value:     event.Entry.Value,
			}); err != nil {
				return err
			}
		}
	}
}

func validateStreamKey(k *pb.StreamKey) (service.StreamKey, error) {
	if k == nil {
		return service.StreamKey{}, status.Error(codes.InvalidArgument, "key is required")
	}
	if k.Group == "" {
		return service.StreamKey{}, status.Error(codes.InvalidArgument, "key.group is required")
	}
	if k.Version == "" {
		return service.StreamKey{}, status.Error(codes.InvalidArgument, "key.version is required")
	}
	if k.Kind == "" {
		return service.StreamKey{}, status.Error(codes.InvalidArgument, "key.kind is required")
	}
	if k.Name == "" {
		return service.StreamKey{}, status.Error(codes.InvalidArgument, "key.name is required")
	}
	return service.StreamKey{
		Group:   k.Group,
		Version: k.Version,
		Kind:    k.Kind,
		Name:    k.Name,
	}, nil
}

func pbStreamKey(k service.StreamKey) *pb.StreamKey {
	return &pb.StreamKey{
		Group:   k.Group,
		Version: k.Version,
		Kind:    k.Kind,
		Name:    k.Name,
	}
}

func toStreamGRPCError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, service.ErrStreamNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, service.ErrStreamValidation) {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	return status.Error(codes.Internal, err.Error())
}
