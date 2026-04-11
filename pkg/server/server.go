// Package server provides the gRPC server implementation.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/authz"
	"github.com/roysav/marketplane/pkg/record"
	"github.com/roysav/marketplane/pkg/service"
)

// Server implements the gRPC RecordService.
type Server struct {
	pb.UnimplementedRecordServiceServer
	svc        *service.Service
	authorizer *authz.Authorizer
	logger     *slog.Logger
}

// New creates a new gRPC server.
func New(svc *service.Service, authorizer *authz.Authorizer, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if authorizer == nil {
		authorizer = authz.New(authz.Config{})
	}
	return &Server{
		svc:        svc,
		authorizer: authorizer,
		logger:     logger.With("component", "grpc"),
	}
}

// Register registers the server with a gRPC server.
func (s *Server) Register(grpcServer *grpc.Server) {
	pb.RegisterRecordServiceServer(grpcServer, s)
}

// Create creates a new record.
func (s *Server) Create(ctx context.Context, req *CreateRequest) (*CreateResponse, error) {
	r, err := pbToRecord(req.Record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid record: %v", err)
	}
	if err := s.authorizer.AuthorizeRecordCreate(ctx, r); err != nil {
		return nil, toGRPCError(err)
	}

	created, err := s.svc.Create(ctx, r)
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbRec, err := recordToPB(created)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert record: %v", err)
	}

	return &CreateResponse{Record: pbRec}, nil
}

// Get retrieves a record by key.
func (s *Server) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	if err := s.authorizer.AuthorizeRecordRead(ctx, authz.VerbGet, req.Type, req.Tradespace); err != nil {
		return nil, toGRPCError(err)
	}
	r, err := s.svc.Get(ctx, req.Type, req.Tradespace, req.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbRec, err := recordToPB(r)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert record: %v", err)
	}

	return &GetResponse{Record: pbRec}, nil
}

// Update updates an existing record.
func (s *Server) Update(ctx context.Context, req *UpdateRequest) (*UpdateResponse, error) {
	r, err := pbToRecord(req.Record)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid record: %v", err)
	}
	current, err := s.svc.Get(ctx, r.TypeMeta.GVK().Type(), r.ObjectMeta.Tradespace, r.ObjectMeta.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	if err := s.authorizer.AuthorizeRecordUpdate(ctx, current, r); err != nil {
		return nil, toGRPCError(err)
	}

	updated, err := s.svc.Update(ctx, r)
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbRec, err := recordToPB(updated)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to convert record: %v", err)
	}

	return &UpdateResponse{Record: pbRec}, nil
}

// Delete removes a record.
func (s *Server) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	if err := s.authorizer.AuthorizeRecordDelete(ctx, req.Type, req.Tradespace); err != nil {
		return nil, toGRPCError(err)
	}
	err := s.svc.Delete(ctx, req.Type, req.Tradespace, req.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &DeleteResponse{}, nil
}

// List returns records matching the query.
func (s *Server) List(ctx context.Context, req *ListRequest) (*ListResponse, error) {
	if err := s.authorizer.AuthorizeRecordRead(ctx, authz.VerbList, req.Type, req.Tradespace); err != nil {
		return nil, toGRPCError(err)
	}
	records, err := s.svc.List(ctx, req.Type, req.Tradespace, req.Labels)
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbRecords := make([]*pb.Record, 0, len(records))
	for _, r := range records {
		pbRec, err := recordToPB(r)
		if err != nil {
			s.logger.Warn("skipping record in list response",
				"type", r.TypeMeta.GVK().Type(),
				"name", r.ObjectMeta.Name,
				"error", err,
			)
			continue
		}
		pbRecords = append(pbRecords, pbRec)
	}

	return &ListResponse{Records: pbRecords}, nil
}

// Watch streams record change events.
func (s *Server) Watch(req *WatchRequest, stream grpc.ServerStreamingServer[WatchEvent]) error {
	ctx := stream.Context()
	if err := s.authorizer.AuthorizeRecordRead(ctx, authz.VerbWatch, req.Type, req.Tradespace); err != nil {
		return toGRPCError(err)
	}

	eventCh, err := s.svc.Watch(ctx, req.Type)
	if err != nil {
		return toGRPCError(err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-eventCh:
			if !ok {
				return nil
			}

			// Parse the event data
			var eventData struct {
				Action     string `json:"action"`
				Type       string `json:"type"`
				Tradespace string `json:"tradespace"`
				Name       string `json:"name"`
			}
			if err := json.Unmarshal([]byte(event.Data), &eventData); err != nil {
				s.logger.Warn("failed to parse event data", "error", err)
				continue
			}
			if req.Tradespace != "" && eventData.Tradespace != req.Tradespace {
				continue
			}

			// Fetch the full record for create/update events
			var pbRec *pb.Record
			if eventData.Action != "deleted" {
				r, err := s.svc.Get(ctx, eventData.Type, eventData.Tradespace, eventData.Name)
				if err == nil {
					pbRec, _ = recordToPB(r)
				}
			}

			if err := stream.Send(&WatchEvent{
				Action: eventData.Action,
				Record: pbRec,
			}); err != nil {
				return err
			}
		}
	}
}

// Type aliases for convenience
type (
	CreateRequest  = pb.CreateRequest
	CreateResponse = pb.CreateResponse
	GetRequest     = pb.GetRequest
	GetResponse    = pb.GetResponse
	UpdateRequest  = pb.UpdateRequest
	UpdateResponse = pb.UpdateResponse
	DeleteRequest  = pb.DeleteRequest
	DeleteResponse = pb.DeleteResponse
	ListRequest    = pb.ListRequest
	ListResponse   = pb.ListResponse
	WatchRequest   = pb.WatchRequest
	WatchEvent     = pb.WatchEvent
)

func pbToRecord(pbRec *pb.Record) (*record.Record, error) {
	if pbRec == nil {
		return nil, errors.New("record is nil")
	}

	var spec, status map[string]any
	if pbRec.Spec != nil {
		spec = pbRec.Spec.AsMap()
	}
	if pbRec.Status != nil {
		status = pbRec.Status.AsMap()
	}

	return &record.Record{
		TypeMeta: record.TypeMeta{
			Group:   pbRec.TypeMeta.GetGroup(),
			Version: pbRec.TypeMeta.GetVersion(),
			Kind:    pbRec.TypeMeta.GetKind(),
		},
		ObjectMeta: record.ObjectMeta{
			Name:            pbRec.ObjectMeta.GetName(),
			Tradespace:      pbRec.ObjectMeta.GetTradespace(),
			Labels:          pbRec.ObjectMeta.GetLabels(),
			Annotations:     pbRec.ObjectMeta.GetAnnotations(),
			ResourceVersion: pbRec.ObjectMeta.GetResourceVersion(),
		},
		Spec:   spec,
		Status: status,
	}, nil
}

func recordToPB(r *record.Record) (*pb.Record, error) {
	var spec, status *structpb.Struct
	var err error

	if r.Spec != nil {
		spec, err = structpb.NewStruct(r.Spec)
		if err != nil {
			return nil, err
		}
	}
	if r.Status != nil {
		status, err = structpb.NewStruct(r.Status)
		if err != nil {
			return nil, err
		}
	}

	return &pb.Record{
		TypeMeta: &pb.TypeMeta{
			Group:   r.TypeMeta.Group,
			Version: r.TypeMeta.Version,
			Kind:    r.TypeMeta.Kind,
		},
		ObjectMeta: &pb.ObjectMeta{
			Name:            r.ObjectMeta.Name,
			Tradespace:      r.ObjectMeta.Tradespace,
			Labels:          r.ObjectMeta.Labels,
			Annotations:     r.ObjectMeta.Annotations,
			ResourceVersion: r.ObjectMeta.ResourceVersion,
			CreatedAt:       timestamppb.New(r.ObjectMeta.CreatedAt),
			UpdatedAt:       timestamppb.New(r.ObjectMeta.UpdatedAt),
		},
		Spec:   spec,
		Status: status,
	}, nil
}

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, service.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, service.ErrAlreadyExists) {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	if errors.Is(err, service.ErrConflict) {
		return status.Error(codes.Aborted, err.Error())
	}
	if errors.Is(err, service.ErrValidation) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, authz.ErrUnauthenticated) {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	if errors.Is(err, authz.ErrPermissionDenied) {
		return status.Error(codes.PermissionDenied, err.Error())
	}
	if errors.Is(err, authz.ErrInvalidRecordUpdate) {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	return status.Error(codes.Internal, err.Error())
}
