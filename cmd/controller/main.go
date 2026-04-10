package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/controller"
)

func main() {
	var (
		serverAddr = flag.String("server", "localhost:50051", "API server address")
		debug      = flag.Bool("debug", false, "Enable debug logging")
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

	// Connect to API server
	logger.Info("connecting to API server", "addr", *serverAddr)
	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to connect to server", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	// Create gRPC clients
	recordsClient := pb.NewRecordServiceClient(conn)
	ledgerClient := pb.NewLedgerServiceClient(conn)

	// Create and run the allocation controller
	ctrl := controller.NewAllocationController(recordsClient, ledgerClient, logger)

	// Handle shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	logger.Info("starting allocation controller")
	if err := ctrl.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("controller error", "error", err)
		os.Exit(1)
	}
}
