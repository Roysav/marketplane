package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/roysav/marketplane/pkg/storage"
	"github.com/roysav/marketplane/pkg/storage/postgres"
	"github.com/roysav/marketplane/pkg/storage/sqlite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/roysav/marketplane/pkg/server"
	"github.com/roysav/marketplane/pkg/service"
	"github.com/roysav/marketplane/pkg/storage/redis"
)

func main() {
	var (
		port      = flag.Int("port", 50051, "gRPC server port")
		dbPath    = flag.String("db", "marketplane.db", "SQLite database path (use :memory: for in-memory)")
		redisAddr = flag.String("redis", "localhost:6379", "Redis address")
		debug     = flag.Bool("debug", false, "Enable debug logging")
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

	// Initialize storage
	logger.Info("initializing storage", "db", *dbPath, "redis", *redisAddr)

	var rows storage.RowStorage
	var ledger storage.LedgerStorage
	var rowsErr, ledgerErr error

	if strings.HasPrefix(*dbPath, "postgres://") {
		rows, rowsErr = postgres.New(ctx, *dbPath)
		ledger, ledgerErr = postgres.NewLedgerStorage(ctx, *dbPath)
	} else {
		ledger, ledgerErr = sqlite.NewLedgerStorage(ctx, *dbPath)
	}

	if rowsErr != nil {
		logger.Error("failed to initialize row storage", "error", rowsErr)
		os.Exit(1)
	}
	defer rows.Close()

	if ledgerErr != nil {
		logger.Error("failed to initialize ledger storage", "error", ledgerErr)
		os.Exit(1)
	}
	defer ledger.Close()

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

	// Initialize services
	svc := service.New(service.Config{
		Rows:   rows,
		Events: events,
		Logger: logger,
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

	ledgerSrv := server.NewLedgerServer(ledger, logger)
	ledgerSrv.Register(grpcServer)

	// Enable reflection for grpcurl/grpcui
	reflection.Register(grpcServer)

	// Start listening
	addr := fmt.Sprintf(":%d", *port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "error", err)
		os.Exit(1)
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
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
