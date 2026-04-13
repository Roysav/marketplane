package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/roysav/marketplane/pkg/record"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/roysav/marketplane/pkg/server"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/postgres"
	"github.com/roysav/marketplane/pkg/storage/redis"
)

func main() {
	var (
		port        = flag.Int("port", 50051, "gRPC server port")
		postgresURL = flag.String("postgres", "", "PostgreSQL connection URL (e.g. postgres://user:pass@localhost:5432/marketplane)")
		redisAddr   = flag.String("redis", "localhost:6379", "Redis address")
		migrate     = flag.Bool("migrate", false, "Run PostgreSQL migrations and exit")
		debug       = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	// Setup logger
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize PostgreSQL
	if *postgresURL == "" {
		panic("--postgres is required")
	}
	pool, err := pgxpool.New(ctx, *postgresURL)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to postgres: %v", err))
	}
	defer pool.Close()

	// Run migrations if requested
	if *migrate {
		logger.Info("running migrations")
		if err := postgres.Migrate(ctx, pool); err != nil {
			panic(fmt.Sprintf("migration failed: %v", err))
		}
		logger.Info("migrations complete")
		return
	}

	// Initialize storage
	logger.Info("initializing storage", "redis", *redisAddr)
	rows := postgres.New(pool)

	// Redis is optional - continue without streams/events if unavailable
	var events *redis.EventStorage
	var streams *redis.StreamStorage
	redisClient, err := redis.NewClient(ctx, redis.Options{Addr: *redisAddr})
	if err != nil {
		logger.Warn("Redis unavailable, streams and events disabled", "error", err)
	} else {
		events = redis.NewEventStorage(redisClient)
		streams = redis.NewStreamStorage(redisClient)
		defer redisClient.Close()
	}

	validator, err := record.NewValidator()
	// Initialize services
	var svc = service.New(service.Config{
		Rows:      rows,
		Events:    events,
		Validator: validator,
		Logger:    logger,
	})
	streamSvc := service.NewStreamService(service.StreamServiceConfig{
		Rows:    rows,
		Streams: streams,
		Logger:  logger,
	})

	// Initialize gRPC server
	grpcServer := grpc.NewServer()
	srv := server.New(svc, logger)
	srv.Register(grpcServer)

	streamSrv := server.NewStreamServer(streamSvc, logger)
	streamSrv.Register(grpcServer)

	// Enable reflection for grpcurl/grpcui
	reflection.Register(grpcServer)

	// Start listening
	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		panic(fmt.Sprintf("failed to listen: %v", err))
	}

	// Handle shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down...")
		grpcServer.GracefulStop()
		cancel()
	}()

	logger.Info("server started", "addr", addr)
	if err := grpcServer.Serve(lis); err != nil {
		panic(fmt.Sprintf("server error: %v", err))
	}
}
