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

	"github.com/roysav/marketplane/pkg/auth"
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

		// TLS flags — when cert+key are provided the server uses TLS.
		// When ca is also provided it enables mutual TLS: clients must present
		// a certificate signed by the given CA. The client cert's CN is mapped
		// to a core/v1/User record name for identity propagation.
		tlsCert = flag.String("cert", "", "Path to PEM-encoded server certificate (enables TLS)")
		tlsKey  = flag.String("key", "", "Path to PEM-encoded server private key (enables TLS)")
		tlsCA   = flag.String("ca", "", "Path to PEM-encoded CA certificate (enables mTLS client auth)")
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

	// Initialize gRPC server — optionally with TLS/mTLS credentials and
	// interceptors that inject the caller's TLS identity into the context.
	var grpcOpts []grpc.ServerOption

	if *tlsCert != "" && *tlsKey != "" {
		creds, err := auth.ServerCredentials(*tlsCert, *tlsKey, *tlsCA)
		if err != nil {
			logger.Error("failed to load TLS credentials", "error", err)
			os.Exit(1)
		}
		grpcOpts = append(grpcOpts,
			grpc.Creds(creds),
			grpc.UnaryInterceptor(auth.NewMiddleware(rows).UnaryInterceptor),
			grpc.StreamInterceptor(auth.NewMiddleware(rows).StreamInterceptor),
		)
		if *tlsCA != "" {
			logger.Info("mTLS enabled", "ca", *tlsCA)
		} else {
			logger.Info("TLS enabled (server-only, no client auth)")
		}
	} else {
		logger.Warn("TLS disabled — running in plaintext mode (use -cert/-key/-ca for production)")
	}

	grpcServer := grpc.NewServer(grpcOpts...)
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
