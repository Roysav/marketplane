package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/roysav/marketplane/api/gen"
	"github.com/roysav/marketplane/pkg/controller"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		serverAddr     = flag.String("server", "localhost:50051", "gRPC server address")
		resyncInterval = flag.Duration("resync", 30*time.Second, "Interval between full resyncs")
		debug          = flag.Bool("debug", false, "Enable debug logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("failed to connect: %v", err))
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	ctrl := controller.NewAllocationController(controller.AllocationControllerConfig{
		Records:        pb.NewRecordServiceClient(conn),
		Ledger:         pb.NewLedgerServiceClient(conn),
		Logger:         logger,
		ResyncInterval: *resyncInterval,
	})

	logger.Info("allocation controller started", "server", *serverAddr, "resync", *resyncInterval)
	if err := ctrl.Run(ctx); err != nil && ctx.Err() == nil {
		panic(fmt.Sprintf("controller failed: %v", err))
	}
}
